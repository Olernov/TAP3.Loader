#include <stdio.h>
#include <time.h>
#include <math.h>
#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cmath>
#include <map>

using namespace std;


struct FtpSetting
{
	string ftpServer;
	string ftpUsername;
	string ftpPassword;
	string ftpPort;
	string ftpDirectory;
};

class Config
{
public:
	Config() {};
	Config(ifstream& cfgStream);

	void ReadConfigFile(ifstream& cfgStream);
	string GetConnectString() const;
	string GetOutputDirectory() const;
	FtpSetting GetFTPSetting(string roamingHub);
private:
	string m_connectString;
	string m_outputDirectory;
	std::map<string, FtpSetting> m_ftpSettings;
};