#pragma once
#include "stdafx.h"
#include "OTL_Header.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "RoamingFileLoader.h"

class TapLoader : public RoamingFileLoader
{
public:
	TapLoader(otl_connect& dbConnect);
	~TapLoader();
	bool ParseFile();
private:
	DataInterChange* dataInterchange;
	
};

