#pragma once
#include "OTL_Header.h"

class RoamingFileLoader
{
public:
	RoamingFileLoader(otl_connect& dbConnect);
	~RoamingFileLoader();
	virtual bool ParseFile() = 0;
	void LoadFileToDB111();
private:
	otl_connect& dbConnect;
	unsigned char* fileBuffer;
};

