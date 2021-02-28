package upf.edu;



import java.io.*;

public class FileLanguageFilter {
	private final File outputFile;

	public FileLanguageFilter (File outputFile) {
		this.outputFile = outputFile;
	}

	public void filterLanguage(SimplifiedTweet filteredTweet)  {

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(this.outputFile, true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
		/*Write Output File*/
		writer.write("\n##########################################################");
		writer.newLine();
		writer.write("Tweet Id: ");
		writer.append(String.valueOf(filteredTweet.getId()));
		
		writer.newLine();
		writer.write("User Id: ");
		writer.append(String.valueOf(filteredTweet.getUserId()));
		
		writer.newLine();
		writer.write("User Name: ");
		writer.append(String.valueOf(filteredTweet.getUserName()));
		
		writer.newLine();
		writer.write("Text: ");
		writer.append(String.valueOf(filteredTweet.getText()));
		
		writer.newLine();
		writer.write("Time Stamp MS: ");
		writer.append(String.valueOf(filteredTweet.getTimeStampMs()));
		writer.newLine();
		writer.write("##########################################################");
		writer.newLine();
		writer.newLine();			
		
		writer.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
							

	}

}
