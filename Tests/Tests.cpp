// DLL_Check.cpp : Defines the entry point for the console application.
//

#include <stdio.h>
#include <conio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <future>
#include "windows.h"
#include "OTL_Header.h"
#include "ConfigContainer.h"
#include "TAP_Constants.h"


const char* ExtractShortName(const char* fullName)
{
	const char *shortName = strrchr(fullName, '\\');
	if (!shortName) {
		shortName = fullName;
	}
	else {
		shortName++;
	}
	return shortName;
}
int main(int argc, const char* argv[])
{
	const char* tapLoaderDLL = "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\DLL Release\\TAP3.Loader.dll";
	const char* sampleFile = "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDRUSNWRUS2700391";
	const long fileID = 1001110;
	const long comfoneHubID = 624467901;
	const char* configFile = "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\Tests.cfg";

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
		otlStream.open(1, "call BILLING.TAP3_TESTS.DisableExchangeRateCheck()", otlConnect);
		otlStream << shortFilename;
		otlStream.close();
		otlConnect.commit();
		
		int(__stdcall * LoadFileToDB) (char*, long, long, char*);
		LoadFileToDB = (int(__stdcall *) (char*, long, long, char*)) GetProcAddress(hinstDLL, "LoadFileToDB");
		if (!LoadFileToDB) {
			std::cout << "Unable to GetProcAddress LoadFileToDB ";
			throw std::exception();
		}
				
		int res = (LoadFileToDB)(const_cast<char*>(sampleFile), fileID, comfoneHubID, const_cast<char*>(configFile));
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
		
		otlStream.open(1, "call BILLING.TAP3_TESTS.ClearPreviousUpload(:filename /*char[20],in*/)", otlConnect);
		otlStream << shortFilename;
		otlStream.close();
		otlStream.open(1, "call BILLING.TAP3_TESTS.EnableExchangeRateCheck()", otlConnect);
		otlStream.close();
		otlStream.open(1, "call BILLING.TAP3_TESTS.EnsureExchangeRateIsWrong()", otlConnect);
		otlStream.close();
		otlConnect.commit();

		res = (LoadFileToDB)(const_cast<char*>(sampleFile), fileID, comfoneHubID, const_cast<char*>(configFile));
		std::cout << "LoadFileToDB result: " << res << std::endl;
		if (res != TL_OK) {
			throw std::exception();
		}
		otlStream.open(1, "call BILLING.TAP3_TESTS.CheckFatalUpload(:file_id /*long,in*/)", otlConnect);
		otlStream << fileID;
		otlStream.close();
		otlStream.open(1, "call BILLING.TAP3_TESTS.ValidateFatalLogging()", otlConnect);
		otlStream.close();

		// multi-threaded tests
		const long sparkleHubID = 935076610;
		const long mtsHubID = 981304980;
		char* sparkleTapFiles[] = { "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDBLR02RUST700001",
			"c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDBLR02RUST700002",
			"c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDBLRMDRUST700001",
			"c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDBLRMDRUST700002",
			"c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDBLRMDRUST700003",
			"c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDKAZKZRUST700001"		
		};
		char* mtsTapFiles[] = { "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\CDRUS01RUS2700248" };
		char* comfoneTapFiles[] = { "c:\\Projects\\TAP3\\TAP3\\TAP3.12_Loader\\Tests\\SampleFiles\\TDLIEK9RUS2700001" };
		
		for (auto& filename : sparkleTapFiles) {
			otlStream.open(1, "call BILLING.TAP3_TESTS.ClearPreviousUpload(:filename /*char[20],in*/)", otlConnect);
			otlStream << ExtractShortName(filename);
			otlStream.close();
		}
		for (auto& filename : mtsTapFiles) {
			otlStream.open(1, "call BILLING.TAP3_TESTS.ClearPreviousUpload(:filename /*char[20],in*/)", otlConnect);
			otlStream << ExtractShortName(filename);
			otlStream.close();
		}
		for (auto& filename : comfoneTapFiles) {
			otlStream.open(1, "call BILLING.TAP3_TESTS.ClearPreviousUpload(:filename /*char[20],in*/)", otlConnect);
			otlStream << ExtractShortName(filename);
			otlStream.close();
		}
		otlConnect.commit();

		std::vector<std::future<int>> results;
		long nextFileID = fileID + 10;
		for (auto& filename : sparkleTapFiles) {
			results.push_back(std::async(launch::async, LoadFileToDB, filename, nextFileID, sparkleHubID, const_cast<char*>(configFile)));
			nextFileID++;
		}
		for (auto& filename : mtsTapFiles) {
			results.push_back(std::async(launch::async, LoadFileToDB, filename, nextFileID, mtsHubID, const_cast<char*>(configFile)));
			nextFileID++;
		}
		for (auto& filename : comfoneTapFiles) {
			results.push_back(std::async(launch::async, LoadFileToDB, filename, nextFileID, comfoneHubID, const_cast<char*>(configFile)));
			nextFileID++;
		}
		
		bool loadFail = false;
		for(auto &res : results) {
			int loadRes = res.get();
	       std::cout << "LoadFileToDB result: " << loadRes << std::endl;
			if (loadRes != TL_OK) {
				loadFail = true;
			}
		}	
		if (loadFail) {
			throw std::exception();
		}

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
		std::cout << "Tests FAILED: " << ex.what() << std::endl;
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

