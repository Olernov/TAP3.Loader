#include "ConfigContainer.h"

void Config::ReadConfigFile(ifstream& configStream)
{
	string line;
	bool processingFtpSettings = false;
	string roamingHubName;
	FtpSetting ftpSetting;
	while (getline(configStream, line))
	{
		size_t pos = line.find_first_not_of(" \t\r\n");
		if (pos != string::npos)
			if (line[pos] == '#' || line[pos] == '\0')
				continue;
		size_t delim_pos = line.find_first_of(" \t=", pos);
		string option_name;
		if (delim_pos != string::npos) 
			option_name = line.substr(pos, delim_pos - pos);
		else
			option_name = line;
		
		transform(option_name.begin(), option_name.end(), option_name.begin(), ::toupper);

		size_t value_pos = line.find_first_not_of(" \t=", delim_pos);
		string option_value;
		if (value_pos != string::npos) {
			option_value = line.substr(value_pos);
			size_t comment_pos = option_value.find_first_of(" \t#");
			if (comment_pos != string::npos)
				option_value = option_value.substr(0, comment_pos);
		}

		if (option_name.compare("CONNECT_STRING") == 0) 
			m_connectString = option_value;

		else if (option_name.compare("OUTPUT_DIRECTORY") == 0) {
			if (option_value[option_value.length() - 1] == '\\')
				option_value.erase(option_value.length() - 1);
			m_outputDirectory = option_value;
		}

		else if (option_name.compare("FTP_SETTINGS_FOR") == 0) {
			roamingHubName = option_value;
			transform(roamingHubName.begin(), roamingHubName.end(), roamingHubName.begin(), ::toupper);
			if (!roamingHubName.empty()) {
				processingFtpSettings = true;
			}
		}

		else if (processingFtpSettings) {
			if (option_name.compare("FTP_SERVER") == 0)
				ftpSetting.ftpServer = option_value;
			else if (option_name.compare("FTP_USERNAME") == 0)
				ftpSetting.ftpUsername = option_value;
			else if (option_name.compare("FTP_PASSWORD") == 0)
				ftpSetting.ftpPassword = option_value;
			else if (option_name.compare("FTP_DIRECTORY") == 0)
				ftpSetting.ftpDirectory = option_value;
			else if (option_name.compare("FTP_PORT") == 0)
				ftpSetting.ftpPort = option_value;
			else if (option_name.compare("END_FTP_SETTINGS") == 0) {
				m_ftpSettings.insert(make_pair(roamingHubName, ftpSetting));
				processingFtpSettings = false;
				// reset temporary objects to prepare them for new settings
				roamingHubName = "";
				ftpSetting.ftpServer = "";
				ftpSetting.ftpUsername = "";
				ftpSetting.ftpPassword = "";
				ftpSetting.ftpDirectory = "";
				ftpSetting.ftpPort = "";
			}
		}
	}	
}

Config::Config(ifstream& configStream)
{
	ReadConfigFile(configStream);
}

string Config::GetConnectString() const
{
	return m_connectString;
}

string Config::GetOutputDirectory() const
{
	return m_outputDirectory;
}

FtpSetting Config::GetFTPSetting(string roamingHub)
{
	transform(roamingHub.begin(), roamingHub.end(), roamingHub.begin(), ::toupper);
	map<string, FtpSetting>::iterator it = m_ftpSettings.find(roamingHub);
	if (it != m_ftpSettings.end()) {
		return it->second;
	}
	else {
		// search for common FTP settings
		it = m_ftpSettings.find("OTHERS");
		if (it != m_ftpSettings.end()) {
			return it->second;
		}
		else {
			return FtpSetting();
		}
	}
}