package org.heigit.bigspatialdata.updater.util.replication;

import java.io.InputStream;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.heigit.bigspatialdata.updater.util.IteratorTmpl;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.xml.v0_6.impl.OsmChangeHandler;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Reads Changes from a XML-ReplicationFile.
 */
public class XmlChangeReaderIterator extends IteratorTmpl<ChangeContainer> implements
    DefaultChangeSink {

  private ChangeContainer change = null;
  private final OsmChangeHandler osmHandler;
  private final XMLStreamReader reader;

  private XmlChangeReaderIterator(XMLStreamReader reader) {
    this.reader = reader;
    this.osmHandler = new OsmChangeHandler(this, true);
  }

  /**
   * Parse an XML-Replication-File into Changes.
   *
   * @param inputStream The data-stream
   * @return An Iterator over ChangeContainers in that input
   * @throws XMLStreamException
   * @throws FactoryConfigurationError
   */
  public static XmlChangeReaderIterator of(InputStream inputStream)
      throws XMLStreamException, FactoryConfigurationError {
    return new XmlChangeReaderIterator(XMLInputFactory.newInstance()
        .createXMLStreamReader(inputStream));
  }

  @Override
  public void process(ChangeContainer change) {
    this.change = change;
  }

  @Override
  protected ChangeContainer getNext() throws Exception {
    change = null;
    while (change == null && reader.hasNext()) {
      int eventType = reader.next();
      String uri;
      String localName;
      String qName;
      AttributesImpl attributes = new AttributesImpl();
      switch (eventType) {
        case XMLStreamReader.START_ELEMENT:
          uri = reader.getNamespaceURI();
          localName = reader.getLocalName();
          qName = reader.getName().toString();

          int attributeCount = reader.getAttributeCount();
          attributes.clear();
          for (int i = 0; i < attributeCount; i++) {
            attributes
                .addAttribute(reader.getAttributeNamespace(i),
                    reader.getAttributeLocalName(i),
                    reader.getAttributeName(i).toString(),
                    reader.getAttributeType(i),
                    reader.getAttributeValue(i));
          }
          osmHandler.startElement(uri, localName, qName, attributes);
          break;
        case XMLStreamReader.ATTRIBUTE:
          System.out.println("XMLSTreamReader.ATTRIBUTE no/op");
          break;
        case XMLStreamReader.END_ELEMENT:
          uri = reader.getNamespaceURI();
          localName = reader.getLocalName();
          qName = reader.getName().toString();
          osmHandler.endElement(uri, localName, qName);
          break;
        default:
          throw new UnsupportedOperationException("Eventtype " + eventType + " unknown.");
      }
    }
    if (!reader.hasNext()) {
      reader.close();
    }
    return change;
  }

}
