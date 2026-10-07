package io.nodusdb.objectstore.s3;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XmlTextTest {

    private static final String LISTING = """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>bucket</Name><Prefix>_nodus%2Fchain%2F</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys>
              <EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>
              <Contents><Key>_nodus%2Fchain%2F1.obj</Key><LastModified>2026-10-07T12:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag><Size>12</Size><StorageClass>STANDARD</StorageClass></Contents>
              <Contents><Key>_nodus%2Fchain%2F2.obj</Key><LastModified>2026-10-07T12:00:01.000Z</LastModified>
                <ETag>&quot;abc&quot;</ETag><Size>34</Size><StorageClass>STANDARD</StorageClass></Contents>
            </ListBucketResult>""";

    @Test
    void everyBlockOfATagIsReturnedInOrder() {
        List<String> contents = XmlText.blocks(LISTING, "Contents");

        assertEquals(2, contents.size());
        assertEquals(Optional.of("_nodus%2Fchain%2F1.obj"), XmlText.text(contents.get(0), "Key"));
        assertEquals(Optional.of("34"), XmlText.text(contents.get(1), "Size"));
    }

    @Test
    void aLeafValueIsReadAndEntitiesAreDecoded() {
        assertEquals(Optional.of("true"), XmlText.text(LISTING, "IsTruncated"));
        assertEquals(Optional.of("\"abc\""), XmlText.text(XmlText.blocks(LISTING, "Contents").get(1), "ETag"));
        assertEquals(Optional.of("a&b<c>"), XmlText.text("<Key>a&amp;b&lt;c&gt;</Key>", "Key"));
    }

    @Test
    void aTagThatIsAPrefixOfAnotherIsNotConfusedWithIt() {
        assertEquals(Optional.of("2"), XmlText.text(LISTING, "KeyCount"));
        assertEquals(List.of(), XmlText.blocks(LISTING, "Ke"));
        assertEquals(Optional.empty(), XmlText.text("<KeyCount>2</KeyCount>", "Key"));
        assertEquals(Optional.of("x"), XmlText.text("<KeyCount>2</KeyCount><Key>x</Key>", "Key"));
    }

    @Test
    void attributesOnTheOpeningTagAreAllowed() {
        assertEquals(Optional.of("v"), XmlText.text("<Root xmlns=\"urn:x\"><Item id=\"1\" kind='a'>v</Item></Root>", "Item"));
        assertEquals(Optional.of("v"), XmlText.text("<Item\n  id=\"1\">v</Item>", "Item"));
    }

    @Test
    void anEmptyAndASelfClosingElementReadAsEmptyText() {
        assertEquals(Optional.of(""), XmlText.text("<Key></Key>", "Key"));
        assertEquals(Optional.of(""), XmlText.text("<Key/>", "Key"));
        assertEquals(Optional.of(""), XmlText.text("<Key />", "Key"));
    }

    @Test
    void anAbsentTagGivesNothing() {
        assertEquals(Optional.empty(), XmlText.text(LISTING, "NextContinuationToken"));
        assertEquals(List.of(), XmlText.blocks("", "Contents"));
    }

    @Test
    void anUnterminatedElementIsIgnoredRatherThanMisread() {
        assertEquals(List.of(), XmlText.blocks("<Contents><Key>a</Key>", "Contents"));
        assertEquals(List.of(), XmlText.blocks("<Contents", "Contents"));
        assertEquals(List.of("<Key>a</Key>"), XmlText.blocks("<Contents><Key>a</Key></Contents><Contents>", "Contents"));
    }

    @Test
    void anErrorDocumentYieldsItsCodeAndMessage() {
        String error = "<?xml version=\"1.0\"?><Error><Code>ConditionalRequestConflict</Code>"
                + "<Message>A conflicting operation is in progress</Message><Resource>/b/k</Resource>"
                + "<RequestId>4442587FB7D0A2F9</RequestId></Error>";

        assertEquals(Optional.of("ConditionalRequestConflict"), XmlText.text(error, "Code"));
        assertEquals(Optional.of("A conflicting operation is in progress"), XmlText.text(error, "Message"));
    }

    @Test
    void numericAndNamedEntitiesAreDecodedAndUnknownOnesAreKept() {
        assertEquals("Aé😀", XmlText.unescape("&#65;&#xE9;&#x1F600;"));
        assertEquals("\"'", XmlText.unescape("&quot;&apos;"));
        assertEquals("&unknown; & &#; &#xZZ; &#99999999;", XmlText.unescape("&unknown; & &#; &#xZZ; &#99999999;"));
        assertEquals("plain", XmlText.unescape("plain"));
        assertEquals("a & b", XmlText.unescape("a & b"));
    }

    @Test
    void escapingProtectsTheCharactersThatBreakMarkup() {
        assertEquals("a&amp;b&lt;c&gt;d\"e'f", XmlText.escape("a&b<c>d\"e'f"));
        assertEquals("plain", XmlText.escape("plain"));
    }

    @Test
    void escapingAndUnescapingAreInverse() {
        String text = "<Delete><Key>a&b</Key></Delete> \"quoted\" 'single' é";

        assertEquals(text, XmlText.unescape(XmlText.escape(text)));
    }
}
