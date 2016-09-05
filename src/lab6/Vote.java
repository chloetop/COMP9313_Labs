package lab6;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class Vote {
	String Id;
	String PostId;
	String VoteTypeId;
	String UserId;
	String CreationDate;

	String getId() {
		return Id;
	}

	String getPostId() {
		return PostId;
	}

	String getVoteTypeId() {
		return VoteTypeId;
	}

	String getUserId() {
		return UserId;
	}

	String getCreationDate() {
		return CreationDate;
	}

	void setId(String value) {
		Id = value;
	}

	void setPostId(String value) {
		PostId = value;
	}

	void setVoteTypeId(String value) {
		VoteTypeId = value;
	}

	void setUserId(String value) {
		UserId = value;
	}

	void setCreationDate(String value) {
		CreationDate = value;
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
			setVoteTypeId(ele.getAttribute("VoteTypeId"));
			if(VoteTypeId.equals("5")){
				setUserId(ele.getAttribute("UserId"));
			} else {
				setUserId(null);
			}
			setCreationDate(ele.getAttribute("CreationDate"));
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public Vote(String line) {
		parse(line.trim());
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Id + " " + PostId + " " + VoteTypeId + " " + UserId + " " + CreationDate);
		return sb.toString();
	}
}
