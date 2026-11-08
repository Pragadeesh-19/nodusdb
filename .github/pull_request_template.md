## What and why

<!-- The problem, and what this change does about it. Link the issue or the TODOS.md item. -->

## How it was tested

<!-- New or changed tests, and how you checked that they fail without the change. -->

## Checklist

- [ ] One idea per change, with its tests.
- [ ] `mvn test` and the Python suite pass. If the change touches threads or anything that waits, the slow-machine gate passes too.
- [ ] No change to a behavior, format or contract recorded in `docs/decisions.md` without agreement in an issue first.
- [ ] A performance claim comes with before and after numbers on the same machine.
- [ ] Commit messages follow `type(scope): summary`.
