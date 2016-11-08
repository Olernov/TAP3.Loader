// DLL_Check.cpp : Defines the entry point for the console application.
//

#include <stdio.h>
#include <conio.h>
#include <iostream>
#include <fstream>
#include "windows.h"
#include "OTL_Header.h"
#include "ConfigContainer.h"
#include "TAP_Constants.h"


int main(int argc, const char* argv[])
{
	const char* tapLoaderDLL = "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\DLL Release\\TAP3.Loader.dll";
	const char* sampleFile = "SampleFiles\\CDRUSNWRUS2700391";
	const long fileID = 1001110;
	const long roamingHubID = 624467901;
	const char* configFile = "Tests.cfg";

	HINSTANCE hinstDLL = nullptr;
	otl_connect otlConnect;
	try {
		typedef VOID(*DLLPROC) (LPTSTR);
		hinstDLL = LoadLibrary(tapLoaderDLL);
		if (!hinstDLL) {
			std::cout << "Unable to load library " << tapLoaderDLL;
			throw std::exception();
		}

		std::ifstream ifsSettings(configFile, ifstream::in);
		if (!ifsSettings.is_open())	{
			std::cout << "Unable to open config file " << configFile;
			throw std::exception();
		}
		Config config;
		config.ReadConfigFile(ifsSettings);
		ifsSettings.close();

		if (config.GetConnectString().empty()) {
			std::cout << "DB connection string not found in file " << configFile;
			throw std::exception();
		}
		// DB connect
		otl_connect::otl_initialize(); // initialize OCI environment
		otlConnect.rlogon(config.GetConnectString().c_str());
		otl_nocommit_stream otlStream;

		const char* shortFilename =  strrchr(sampleFile,'\\');
		if(!shortFilename)
			shortFilename = sampleFile;
		else
			shortFilename++;

		otlStream.open(1, "call BILLING.TAP3_TESTS.ClearPreviousUpload(:filename /*char[20],in*/)", otlConnect);
		otlStream << shortFilename;
		otlStream.close();
		otlConnect.commit();
		
		int(__stdcall * LoadFileToDB) (char*, long, long, char*);
		LoadFileToDB = (int(__stdcall *) (char*, long, long, char*)) GetProcAddress(hinstDLL, "LoadFileToDB");
		if (!LoadFileToDB) {
			std::cout << "Unable to GetProcAddress LoadFileToDB ";
			throw std::exception();
		}
				
		int res = (LoadFileToDB)(const_cast<char*>(sampleFile), fileID, roamingHubID, const_cast<char*>(configFile));
		std::cout << "LoadFileToDB result: " << res << std::endl;
		if (res != TL_OK) {
			throw std::exception();
		}
		otlStream.open(1, "call BILLING.TAP3_TESTS.CompareUploadToSample(:filename /*char[20],in*/)", otlConnect);
		otlStream << shortFilename;
		otlStream.close();

		otlStream.open(1, "call BILLING.TAP3_TESTS.ValidateLogging(:filename /*char[20],in*/, :file_id /*long,in*/)", otlConnect);
		otlStream << shortFilename;
		otlStream << fileID;
		otlStream.close();

		std::cout << "Tests PASSED. " << std::endl; 
	}
	catch (otl_exception &otlEx) {
		otlConnect.rollback();
		std::cout << "DB error: " << (char*)otlEx.msg << std::endl;
		if (strlen(otlEx.stm_text) > 0)
			std::cout << (char*)otlEx.stm_text << std::endl; 
		if (strlen(otlEx.var_info) > 0)
			std::cout << (char*)otlEx.var_info << std::endl; 
		std::cout << "Tests FAILED" << std::endl;
	}
	catch(const std::exception& ex) {
		std::cout << "Tests FAILED" << std::endl;
	}
	if (otlConnect.connected) {
		otlConnect.commit();
		otlConnect.logoff();
	}
	if (hinstDLL != nullptr) {
		FreeLibrary(hinstDLL);
	}
	std::cout << "Enter anything to exit." << std::endl; 
	char dummy;
	std::cin >> dummy;
}

