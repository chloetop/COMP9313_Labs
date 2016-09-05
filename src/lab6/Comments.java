package lab6;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Comments {
	String Id;
	String PostId;
	String Score;
	String Text;
	String UserId;

	String getId() {
		return Id;
	}

	String getPostId() {
		return PostId;
	}

	String getScore() {
		return Score;
	}

	String getText() {
		return Text;
	}
	
	String getUserId() {
		return UserId;
	}

	
	void setId(String value) {
		Id = value;
	}

	void setPostId(String value) {
		PostId = value;
	}

	void setScore(String value) {
		Score = value;
	}


	void setTextDate(String value) {
		Text = value;
	}
	
	void setUserId(String value) {
		UserId = value;
	}

	/* You can also use regular expression, e.g.:
	 * private void parse(String line){ 
	 * 		Pattern p = Pattern.compile("([a-zA-Z]+)=\"([^\\s]+)\""); 
	 * 		Matcher m = p.matcher(line); 
	 * 		while(m.find()){ System.out.println(m.group()); } 
	 * }
	 */
	
	private void parse(String line) {

		try {
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

			InputStream in = IOUtils.toInputStream(line, "UTF-8");
			Document doc = dBuilder.parse(in);
			
			Element ele = (Element) doc.getElementsByTagName("row").item(0);
			setId(ele.getAttribute("Id"));
			setPostId(ele.getAttribute("PostId"));
			setScore(ele.getAttribute("Score"));
			setTextDate(ele.getAttribute("Text"));
			setUserId(ele.getAttribute("UserId"));
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public Comments(String line) {
		parse(line.trim());
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Id + " " + PostId + " " + Score + " " + Text + " " + UserId);
		return sb.toString();
	}
}
