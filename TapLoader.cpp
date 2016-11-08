#include "stdafx.h"
#include "TapLoader.h"


TapLoader::TapLoader(otl_connect& dbConnect) :
	RoamingFileLoader(dbConnect)
{
}


TapLoader::~TapLoader()
{
}


bool TapLoader::ParseFile()
{
	return true;
}