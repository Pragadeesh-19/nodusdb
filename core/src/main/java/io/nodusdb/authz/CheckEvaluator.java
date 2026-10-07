package io.nodusdb.authz;

import io.nodusdb.authz.schema.CompiledSchema;
import io.nodusdb.error.CheckDepthException;
import io.nodusdb.kernel.GraphKernel;
import io.nodusdb.kernel.Partition;
import io.nodusdb.kernel.adjacency.EdgeKey;

import java.util.Arrays;

final class CheckEvaluator {

    private static final int FRAME_INTS = 4;
    private static final int NODE = 0;
    private static final int DEFINITION = 1;
    private static final int CURSOR = 2;
    private static final int TERM = 3;
    private static final int PROBE_PENDING = -1;
    private static final int INITIAL_FRAMES = 16;
    private static final int FOUND = -2;
    private static final int EXHAUSTED = -3;
    private static final int DESCENDED = -4;

    private final GraphKernel kernel;
    private final NodeTypes types;
    private final int depthLimit;
    private final FrameSet visited = new FrameSet();
    private int[] stack = new int[INITIAL_FRAMES * FRAME_INTS];
    private int frames;
    private boolean depthExceeded;

    CheckEvaluator(GraphKernel kernel, NodeTypes types, int depthLimit) {
        this.kernel = kernel;
        this.types = types;
        this.depthLimit = depthLimit;
    }

    boolean check(CompiledSchema model, int objectNode, int definition, int subjectNode) {
        while (true) {
            long started = kernel.readStart();
            try {
                boolean found = search(model, objectNode, definition, subjectNode);
                if (kernel.readStillValid(started)) {
                    return found;
                }
            } catch (CheckDepthException e) {
                if (kernel.readStillValid(started)) {
                    throw e;
                }
            }
        }
    }

    private boolean search(CompiledSchema model, int objectNode, int definition, int subjectNode) {
        visited.clear();
        frames = 0;
        depthExceeded = false;
        push(model, objectNode, definition);
        while (frames > 0) {
            int base = (frames - 1) * FRAME_INTS;
            int outcome = model.isStored(stack[base + DEFINITION])
                    ? stepStored(model, base, subjectNode) : stepPermission(model, base);
            if (outcome == FOUND) {
                return true;
            }
            if (outcome == EXHAUSTED) {
                frames--;
            }
        }
        if (depthExceeded) {
            throw new CheckDepthException("the check reached the depth limit of " + depthLimit
                    + " before it could decide");
        }
        return false;
    }

    private int stepStored(CompiledSchema model, int base, int subjectNode) {
        int node = stack[base + NODE];
        int definition = stack[base + DEFINITION];
        int relation = model.relationIdOf(definition);
        if (stack[base + CURSOR] == PROBE_PENDING) {
            if (kernel.probeTuple(node, relation, 0, subjectNode)) {
                return FOUND;
            }
            stack[base + CURSOR] = 0;
        }
        int degree = kernel.probeDegree(Partition.INDIRECT, node);
        for (int i = stack[base + CURSOR]; i < degree; i++) {
            long key = kernel.probeKey(Partition.INDIRECT, node, i);
            int userset = EdgeKey.subjectRelation(key);
            if (EdgeKey.relation(key) == relation && userset != 0) {
                int target = model.definitionOfRelation(userset);
                if (target != CompiledSchema.NO_DEF && push(model, EdgeKey.id(key), target)) {
                    stack[base + CURSOR] = i + 1;
                    return DESCENDED;
                }
            }
        }
        return EXHAUSTED;
    }

    private int stepPermission(CompiledSchema model, int base) {
        int node = stack[base + NODE];
        int definition = stack[base + DEFINITION];
        int terms = model.termCount(definition);
        while (stack[base + TERM] < terms) {
            int term = stack[base + TERM];
            if (model.termKind(definition, term) == CompiledSchema.COMPUTED) {
                stack[base + TERM] = term + 1;
                if (push(model, node, model.computedTarget(definition, term))) {
                    return DESCENDED;
                }
            } else if (descendThroughTupleset(model, base, definition, term)) {
                return DESCENDED;
            } else {
                stack[base + TERM] = term + 1;
                stack[base + CURSOR] = 0;
            }
        }
        return EXHAUSTED;
    }

    private boolean descendThroughTupleset(CompiledSchema model, int base, int definition, int term) {
        int node = stack[base + NODE];
        int tupleset = model.tuplesetRelation(definition, term);
        int degree = kernel.probeDegree(Partition.INDIRECT, node);
        for (int i = stack[base + CURSOR]; i < degree; i++) {
            long key = kernel.probeKey(Partition.INDIRECT, node, i);
            if (EdgeKey.relation(key) != tupleset || EdgeKey.subjectRelation(key) != 0) {
                continue;
            }
            int child = EdgeKey.id(key);
            int type = types.typeOf(child, model);
            int target = type == CompiledSchema.NO_DEF ? CompiledSchema.NO_DEF
                    : model.arrowTarget(definition, term, type);
            if (target != CompiledSchema.NO_DEF && push(model, child, target)) {
                stack[base + CURSOR] = i + 1;
                return true;
            }
        }
        return false;
    }

    private boolean push(CompiledSchema model, int node, int definition) {
        if (!visited.add(node, definition)) {
            return false;
        }
        if (frames >= depthLimit) {
            depthExceeded = true;
            return false;
        }
        if ((frames + 1) * FRAME_INTS > stack.length) {
            stack = Arrays.copyOf(stack, stack.length * 2);
        }
        int base = frames * FRAME_INTS;
        stack[base + NODE] = node;
        stack[base + DEFINITION] = definition;
        stack[base + CURSOR] = model.isStored(definition) ? PROBE_PENDING : 0;
        stack[base + TERM] = 0;
        frames++;
        return true;
    }
}
