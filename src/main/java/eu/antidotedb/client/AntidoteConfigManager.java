package eu.antidotedb.client;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Salman on 3/5/2017.
 */

public class AntidoteConfigManager {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8087;
    public static final String DEFAULT_FILE = "config.xml";

    // Checks if the file exists and is not a directory
    private boolean configFileExist(String path) {
        File tmpFile = new File(path);
        return (tmpFile.exists() && tmpFile.isFile());
    }

    public boolean configExist() {
        String cfgPath = System.getProperty("user.dir") + "/" + this.DEFAULT_FILE;
        return this.configFileExist(cfgPath);
    }

    public boolean configExist(String cfgPath) {
        return this.configFileExist(cfgPath);
    }

    // Generate config file from default values
    public void generateDefaultConfig() {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("config");

            Element host = doc.createElement("host");
            Element hostname = doc.createElement("hostname");
            hostname.appendChild(doc.createTextNode(this.DEFAULT_HOST));
            host.appendChild(hostname);

            Element port = doc.createElement("port");
            port.appendChild(doc.createTextNode(String.valueOf(this.DEFAULT_PORT)));
            host.appendChild(port);

            rootElement.appendChild(host);
            doc.appendChild(rootElement);

            String cfgPath = System.getProperty("user.dir") + "/" + this.DEFAULT_FILE;

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(cfgPath));
            transformer.transform(source, result);
        } catch (TransformerException | ParserConfigurationException ex) {
            throw new AntidoteException("Could not create default config file", ex);
        }
    }

    // Checks if the passed document is valid for config file parsing
    private boolean isValidDocument(Document dc) {
        String rootNode = dc.getDocumentElement().getNodeName();
        if (!rootNode.equals("config")) {
            // Invalid config file root element
            return false;
        }
        NodeList hostNodes = dc.getElementsByTagName("host");
        boolean retVal = true;
        for (int i = 0; i < hostNodes.getLength(); i++) {
            Node n = hostNodes.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) {
                retVal = false;
                break;
            }
            Element ele = (Element) n;
            retVal = (
                    !ele.getElementsByTagName("hostname").item(0).getTextContent().isEmpty() ||
                            !ele.getElementsByTagName("port").item(0).getTextContent().isEmpty()
            );
        }
        return retVal;
    }

    public List<Host> getConfigHosts() {
        String cfgPath = System.getProperty("user.dir") + "/" + DEFAULT_FILE;
        return getConfigHosts(cfgPath);
    }

    public List<Host> getConfigHosts(String filepath) {
        List<Host> list = new LinkedList<Host>();
        String cfgPath = filepath;
        if (!this.configFileExist(cfgPath)) {
            // No File Found, returning default values
            list.add(new Host(DEFAULT_HOST, DEFAULT_PORT));
            return list;
        }
        File config = new File(cfgPath);
        Document doc;
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbFactory.newDocumentBuilder();
            doc = db.parse(config);
            doc.getDocumentElement().normalize();
            if (!isValidDocument(doc)) {
                throw new AntidoteException("Invalid document type.");
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new AntidoteException("Error when reading config file " + config, e);
        }
        NodeList hostNodes = doc.getElementsByTagName("host");
        for (int i = 0; i < hostNodes.getLength(); i++) {
            Node n = hostNodes.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element ele = (Element) n;
                String hostname = ele.getElementsByTagName("hostname").item(0).getTextContent();
                String portSt = ele.getElementsByTagName("port").item(0).getTextContent();
                int port = Integer.parseInt(portSt);
                Host h = new Host(hostname, port);
                list.add(h);
            }
        }
        return list;
    }
}
