// TAP3.12c.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "OTL_Header.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "Acknowledgement.h"

#include "TAP_Constants.h"
#include "ConfigContainer.h"
#include "TAPValidator.h"


const char *pShortName;
unsigned char* buffer = NULL;		// буфер для загрузки содержимого файла
long TapFileID;				// ID TAP-файла в таблице TAP3_FILE
uint64_t totalCharge=0;			// суммарное начисление (считается без деления на dblTAPPower)
int totalCallDetailCount=0;
int debugMode = 0;
DataInterChange* dataInterchange = NULL;
ReturnBatch* returnBatch = NULL;
Acknowledgement* acknowledgement = NULL;
Config config;

otl_connect otlConnect;
otl_connect otlLogConnect;
ofstream ofsLog;

const short mainArgsCount = 5;

enum FileType {
	ftTAP = 0,
	ftRAP = 1,
	ftRAPAcknowledgement = 2
};

//extern "C" int _search4tag(const void *ap, const void *bp);

//-----------------------------
void logToFile(string message)
{
	time_t t = time(0);   // get time now
    struct tm * now = localtime( & t );
	
	(ofsLog.is_open() ? ofsLog : cout) << now->tm_mday << '.' << (now->tm_mon + 1) << '.' << (now->tm_year + 1900) << ' ' 
		<< now->tm_hour << ':' << now->tm_min << ':' << now->tm_sec << ' ' << message << endl;
}
//-----------------------------
void log(string filename, short msgType, string msgText)
{
	otl_stream otlLog;
	
	if (otlLogConnect.connected) {
		if (msgText.length() > 2048)
			msgText = msgText.substr(0, 2048);
		try {
			otlLog.open(1, "insert into BILLING.TAP3LOADER_LOG (datetime, filename, msg_type, msg_text) "
				"values (sysdate, :fn /*char[255]*/, :msg_type/*short*/, :msg_text /*char[2048]*/)", otlLogConnect);
			otlLog << filename << msgType << msgText;
		}
		catch (otl_exception &otlEx) {
			string msg = "Unable to write log message to DB: ";
			msg += msgText;
			logToFile(msg);
			logToFile((char*) otlEx.msg);
			if (strlen(otlEx.stm_text) > 0)
				logToFile((char*) otlEx.stm_text); // log SQL that caused the error
			if (strlen(otlEx.var_info) > 0)
				logToFile((char*) otlEx.var_info); // log the variable that caused the error
		}
	}
	else {
		logToFile(to_string(static_cast<unsigned long long> ( msgType )) + '\t' + msgText);
	}
}
//------------------------------
void log(short msgType, string msgText)
{
	log(pShortName, msgType, msgText);
}
//--------------------------------
int assign_integer_option(string _name, string _value, long& param, long minValid, long maxValid)
{
	size_t stoi_idx;
	try {
		param = stoi(_value, &stoi_idx);
	}
	catch(...) {
		cerr << "Wrong value for " << _name << " given in ini-file.";
		return -1;
	}
	if(stoi_idx < _value.length()) {
		cerr << "Wrong value for " << _name << " given in ini-file.";
		return -1;
	}
	if(param<minValid || param>maxValid) {
		cerr << "Wrong value for " << _name << " given in ini-file. Valid range is from " << minValid << " to " << maxValid;
		return -1;
	}
	
	return 0;
}
//----------------------------
int write_out(const void *buffer, size_t size, void *app_key) {
	FILE *out_fp = (FILE*) app_key;
	size_t wrote = fwrite(buffer, 1, size, out_fp);
	return (wrote == size) ? 0 : -1;
}
//-----------------------------
string BCDString(BCDString_t* src, bool bSwitchDigits=false)
{
	string dest;
	
	if(src) {
		for(int i=0; i<src->size; i++) {
			if( bSwitchDigits ) {
				dest.push_back(src->buf[i] & 0x0F);
				dest.push_back((src->buf[i] & 0xF0) >> 4);
			}
			else {
				dest.push_back((src->buf[i] & 0xF0) >> 4);
				dest.push_back(src->buf[i] & 0x0F);
			}
		}

		for(unsigned int i=0; i<dest.size(); i++)
			if(dest[i]<0xA)
			{	
				dest[i]+='0';
			}
			else
			{
				switch( dest[i] )
				{
				case 0xA: dest[i]='*';
						break;
				case 0xB: dest[i]='*';
						break;
				case 0xC: dest[i]='#';
						break;
				case 0xD: dest[i]='a';
						break;
				case 0xE: dest[i]='b';
						break;
				case 0xF: //dest[i]='c';
					dest.erase(i--, 1);
				}
			}
	}
	return dest;
}	
//------------------------------
long long OctetStr2Int64(const OCTET_STRING_t& octetStr)
{
	long long value=0;

	if( octetStr.size > 8 )
		throw "8-byte integer overflow";

	for(int i=0; i < octetStr.size; i++) {
		value <<= 8;
		value |= octetStr.buf[i];
	}

	return value;
}
//----------------------------------
double GetTAPPower()
{
	return pow((double) 10, *dataInterchange->choice.transferBatch.accountingInfo->tapDecimalPlaces);
}

string GetUTCOffset(int nCode)
{
	if (dataInterchange) {
		for (int i = 0; i < dataInterchange->choice.transferBatch.networkInfo->utcTimeOffsetInfo->list.count; i++)
		{
			if (*dataInterchange->choice.transferBatch.networkInfo->utcTimeOffsetInfo->list.array[i]->utcTimeOffsetCode == nCode)
				return (string)(const char*)dataInterchange->choice.transferBatch.networkInfo->utcTimeOffsetInfo->list.array[i]->utcTimeOffset->buf;
		}
		// код UTC offset не найден
		return "???";
	}
	else {
		// объект dataInterchange не инициализирован, возможно происходит загрузка RAP-файла
		return to_string(static_cast<unsigned long long> (nCode));
	}
}
//-------------------------------
string GetRecordingEntity(int nCode, string& recEntityType)
{
	if (dataInterchange) {
		for (int i = 0; i < dataInterchange->choice.transferBatch.networkInfo->recEntityInfo->list.count; i++)
		{
			if (*dataInterchange->choice.transferBatch.networkInfo->recEntityInfo->list.array[i]->recEntityCode == nCode)
			{
				switch (*dataInterchange->choice.transferBatch.networkInfo->recEntityInfo->list.array[i]->recEntityType) {
				case 1:
					recEntityType = "MSC";
					break;
				case 2:
					recEntityType = "SMSC";
					break;
				case 3:
					recEntityType = "GGSN/P-GW";
					break;
				case 4:
					recEntityType = "SGSN";
					break;
				case 5:
					recEntityType = "GMLC";
					break;
				case 6:
					recEntityType = "Wi-Fi";
					break;
				case 7:
					recEntityType = "P-GW";
					break;
				case 8:
					recEntityType = "S-GW";
					break;
				case 9:
					recEntityType = "P-CSCF";
					break;
				case 10:
					recEntityType = "TRF";
					break;
				case 11:
					recEntityType = "ATCF";
					break;
				}
				return (const char*)dataInterchange->choice.transferBatch.networkInfo->recEntityInfo->list.array[i]->recEntityId->buf;
			}

		}

		// неверный код емкости
		throw "e_wrong_rec_entity_code";
	}
	else {
		// объект dataInterchange не инициализирован, возможно происходит загрузка RAP-файла
		recEntityType = "";
		return "Code: " + to_string(static_cast<unsigned long long> (nCode));
	}
}
//-------------------------------
double GetExRate(int nCode)
{
	if (dataInterchange) {
		for(int i=0; i < dataInterchange->choice.transferBatch.accountingInfo->currencyConversionInfo->list.count ; i++)
		{	
			if( *dataInterchange->choice.transferBatch.accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode == nCode)
			{	
				double dblPower=pow( (double) 10, *dataInterchange->choice.transferBatch.accountingInfo->currencyConversionInfo->list.array[i]->numberOfDecimalPlaces);
				return *dataInterchange->choice.transferBatch.accountingInfo->currencyConversionInfo->list.array[i]->exchangeRate / dblPower;
			}
		}
		// неверный код exchange rate
		throw "e_wrong_ex_rate_code";
	}
	else {
		// объект dataInterchange не инициализирован, возможно происходит загрузка RAP-файла
		return 0;
	}
}
//-------------------------------
double GetTaxRate(int nCode)
{
	if (dataInterchange) {
		char * pend;
		if(!dataInterchange->choice.transferBatch.accountingInfo->taxation) 
			return 0;
	
		for(int i=0; i < dataInterchange->choice.transferBatch.accountingInfo->taxation->list.count ; i++)
		{	
			if( *dataInterchange->choice.transferBatch.accountingInfo->taxation->list.array[i]->taxCode == nCode)
				return strtol((const char*)dataInterchange->choice.transferBatch.accountingInfo->taxation->list.array[i]->taxRate->buf, &pend, 10) / 100000 / 100; // The rate is given to 5 decimal places, see TD.57 v32 and expresses in percents
		}
		// неверный код exchange rate
		throw "e_wrong_tax_rate_code";
	}
	else {
		// объект dataInterchange не инициализирован, возможно происходит загрузка RAP-файла
		return 0;
	}
}
//-------------------------------
double GetDiscountRate(int nCode, double& fixedDiscountVal)
{
	if (dataInterchange) {
		if(!dataInterchange->choice.transferBatch.accountingInfo->discounting) {
			fixedDiscountVal = 0;
			return 0;
		}
	
		for(int i=0; i < dataInterchange->choice.transferBatch.accountingInfo->discounting->list.count ; i++)
		{	
			if( *dataInterchange->choice.transferBatch.accountingInfo->discounting->list.array[i]->discountCode == nCode) {
				if( dataInterchange->choice.transferBatch.accountingInfo->discounting->list.array[i]->discountApplied->present == DiscountApplied_PR_discountRate ) {
					fixedDiscountVal = -1;
					return dataInterchange->choice.transferBatch.accountingInfo->discounting->list.array[i]->discountApplied->choice.discountRate / 100; // discount rate is held in 2 decimal places, see TD.57 v32
				}
				else {
					fixedDiscountVal = OctetStr2Int64(dataInterchange->choice.transferBatch.accountingInfo->discounting->list.array[i]->discountApplied->choice.fixedDiscountValue) / GetTAPPower();
					return -1;
				}
			}
		}
		// неверный код exchange rate
		throw "e_wrong_tax_rate_code";
	}
	else {
		// объект dataInterchange не инициализирован, возможно происходит загрузка RAP-файла
		return 0;
	}
}
//-------------------------------
long ProcessChrInfo(long long eventID, ChargeInformation* chargeInformation, char* szInfo)
{
	// обработаем набор Charge Information
	long chargeID;
	// проверка наличия обязательных структур в Charge Information
	if(!chargeInformation->chargedItem || /*!chargeInformation->exchangeRateCode ||*/ !chargeInformation->chargeDetailList)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Charge Information. " + string(szInfo));
		return TL_MISSINGSTRUCT;
	}

	// проверка наличия обязательных структур в Charge Information/Call Type Group
	if(chargeInformation->callTypeGroup &&
		(!chargeInformation->callTypeGroup->callTypeLevel1 || !chargeInformation->callTypeGroup->callTypeLevel2 || !chargeInformation->callTypeGroup->callTypeLevel3))
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Charge Information/Call Type Group. " + string(szInfo));
		return TL_MISSINGSTRUCT;
	}

	otl_nocommit_stream otlStream;
	double fixedDiscountValue = 0;

	otlStream.open( 1 , "INSERT INTO BILLING.TAP3_CHARGEINFO (CHARGE_ID,EVENT_ID,CHR_ITEM,EXCHANGE_RATE,CT_LEVEL1,CT_LEVEL2,CT_LEVEL3, \
		TAX_RATE,TAX_VAL,DISCOUNT_RATE,FIXED_DISCOUNT_VALUE, DISCOUNT_VALUE) VALUES (\
		BILLING.TAP3EVENTID.NextVal, :hEventid /*bigint,in*/, :hChritem /*char[10],in*/, :hExrate /*double,in*/, :hCtlevel1 /*long,in*/, :hCtlevel2 /*long,in*/, :hCtlevel3 /*long,in*/, \
		:hTaxrate /*double,in*/,:hTaxval /*double,in*/, :hDiscrate /*double,in*/, :hFixedDiscval /*double,in*/, :hDiscountVal /*double,in*/) \
		returning CHARGE_ID into :hChargeId /*long,out*/", otlConnect );
	otlStream
		<< eventID
		<< chargeInformation->chargedItem->buf;

	if (chargeInformation->exchangeRateCode )
		otlStream << GetExRate( *chargeInformation->exchangeRateCode );
	else
		otlStream << otl_null();
	
	if (chargeInformation->callTypeGroup ) 
		otlStream 
			<< *chargeInformation->callTypeGroup->callTypeLevel1
			<< *chargeInformation->callTypeGroup->callTypeLevel2
			<< *chargeInformation->callTypeGroup->callTypeLevel3;
	else 
		otlStream 
			<< otl_null()
			<< otl_null()
			<< otl_null();

	double dblTAPPower=pow( (double) 10, dataInterchange ? *dataInterchange->choice.transferBatch.accountingInfo->tapDecimalPlaces :
															*returnBatch->rapBatchControlInfoRap.tapDecimalPlaces);
	if ( chargeInformation->taxInformation )
		otlStream
			<< GetTaxRate( *chargeInformation->taxInformation->list.array[0]->taxCode )
			<< OctetStr2Int64(*chargeInformation->taxInformation->list.array[0]->taxValue) / dblTAPPower;
	else
		otlStream 
			<< otl_null()
			<< otl_null();

	if (chargeInformation->discountInformation ) {
		double discountRate = GetDiscountRate( *chargeInformation->discountInformation->discountCode, fixedDiscountValue );
		if ( discountRate > -1 )
			otlStream << discountRate;
		else
			otlStream << otl_null();

		if ( fixedDiscountValue > -1 )
			otlStream << fixedDiscountValue;
		else
			otlStream << otl_null();

		if (chargeInformation->discountInformation->discount)
			otlStream << (double) (OctetStr2Int64(*chargeInformation->discountInformation->discount) / dblTAPPower);
		else
			otlStream << otl_null();
	}
	else
		otlStream 
			<< otl_null()
			<< otl_null()
			<< otl_null();
		
	otlStream.flush();
	otlStream >> chargeID;
	otlStream.close();

	// обработаем набор Charge Detail
	//long det_res;
	for(int chdet_ind=0; chdet_ind<chargeInformation->chargeDetailList->list.count; chdet_ind++)
	{
		if(chargeInformation->chargeDetailList->list.array[chdet_ind]->charge)
			if ( !strcmp( (const char*) chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeType->buf, "00" ))
				totalCharge += OctetStr2Int64(*chargeInformation->chargeDetailList->list.array[chdet_ind]->charge);
		
		otlStream.open( 1 , "INSERT INTO BILLING.TAP3_CHARGEDETAIL (CHARGE_ID,CHR_TYPE,CHARGE,CHARGEABLE_UNITS,CHARGED_UNITS,DETAIL_TIME,DETAIL_UTCOFF) \
							VALUES ( :hChargid /* long */, :hChr_type /* char[5] */, :hCharge /* double */, :hChrable /* bigint */, :hCharged /* bigint */, \
							to_date(:hDet_time /* char[20] */,'yyyymmddhh24miss'), :hDet_utc /* char[10] */)", otlConnect);
		otlStream
			<< chargeID
			<< (const char*) chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeType->buf
			<< OctetStr2Int64( *chargeInformation->chargeDetailList->list.array[chdet_ind]->charge ) / dblTAPPower;

		if ( chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeableUnits )
			otlStream << OctetStr2Int64( *chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeableUnits );
		else
			otlStream << otl_null();

		if (chargeInformation->chargeDetailList->list.array[chdet_ind]->chargedUnits )
			otlStream << OctetStr2Int64( *chargeInformation->chargeDetailList->list.array[chdet_ind]->chargedUnits );
		else
			otlStream << otl_null();

		otlStream 
			<< (chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeDetailTimeStamp ? 
					(const char*) chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeDetailTimeStamp->localTimeStamp->buf : "")
			<< (chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeDetailTimeStamp ? 
				GetUTCOffset(*chargeInformation->chargeDetailList->list.array[chdet_ind]->chargeDetailTimeStamp->utcTimeOffsetCode) : "");
		
		otlStream.flush();
		otlStream.close();
	}
	
	return TL_OK;
}
//------------------------------------------------------
long long ProcessOriginatedCall(long fileID, int index, const MobileOriginatedCall* pMCall)
{
	// проверка наличия обязательных структур в Mobile Originated Call
	if(!pMCall->basicCallInformation || !pMCall->locationInformation || !pMCall->basicServiceUsedList)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Originated Call. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}
	// проверка наличия обязательных структур в Mobile Originated Call/MO Basic Call Information
	if(!pMCall->basicCallInformation->chargeableSubscriber || !pMCall->basicCallInformation->callEventStartTimeStamp ||
		!pMCall->basicCallInformation->totalCallEventDuration || !pMCall->basicCallInformation->callEventStartTimeStamp)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Originated Call/MO Basic Call Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	// проверка наличия обязательных структур в Mobile Originated Call/Location Information
	if(!pMCall->locationInformation->networkLocation  ||
		!pMCall->locationInformation->networkLocation->recEntityCode)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Originated Call/MO Basic Call Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	long long eventID;
	string recEntityType;

	otl_nocommit_stream otlStream;	
	
	otlStream.open( 1 /*stream buffer size in logical rows*/, 
		"insert into BILLING.TAP3_CALL (EVENT_ID,FILE_ID,RSN,ORIG_OR_TERM,IMSI,MSISDN,PARTY_NUMBER, DIALLED_DIGITS, THIRD_PARTY, SMS_PARTYNUMBER, \
			CLIR,PARTY_NETWORK,CALL_TIME,CALL_UTCOFF,DURATION,CAUSE_FOR_TERM,REC_ENTITY,REC_ENTITY_TYPE,\
			LOCATION_AREA,CELL_ID,SERVING_NETWORK,IMEI, RAP_FILE_SEQNUM ) VALUES (\
			BILLING.Origin_Seq.NextVal, :hFileID /* long,in */, :hIndex /* int,in */, :hOrigorterm /*short,in*/, :hImsi /* char[20],in */, :hMsisdn  /* char[20],in */, \
			:hCallednum  /* char[40],in */, :hDialledDigits  /* char[40],in */, :hThirdParty /* char[40],in */, :hSMSDestination /* char[40],in */, \
			:hClir  /* short,in */, :hPartynetw  /* char[20],in */ , \
			to_date(:hCalltime  /* char[20],in */,'yyyymmddhh24miss'), :hCall_utc /* char[10],in */,:hDuration  /* long,in */ ,:hCause  /* long,in */,\
			:hRecentity /* char[30],in */, :hRecentityType /* char[10],in */, :hLocarea /* long,in */, :hCellid /* long,in */, \
			:hServnetw /* char[20],in */, :hImei /* char[30],in */, :hRAPSeqnum /* char[10],in */) returning EVENT_ID into :hEventId /*bigint,out*/", otlConnect);
	otlStream 
		<< fileID
		<< index 
		<< (short) 1
		<< BCDString( pMCall->basicCallInformation->chargeableSubscriber->choice.simChargeableSubscriber.imsi )
		<< BCDString( pMCall->basicCallInformation->chargeableSubscriber->choice.simChargeableSubscriber.msisdn )
		<< (pMCall->basicCallInformation->destination ? 
			(pMCall->basicCallInformation->destination->calledNumber ? BCDString(pMCall->basicCallInformation->destination->calledNumber) : "") : "")
		<< (pMCall->basicCallInformation->destination ? 
		(pMCall->basicCallInformation->destination->dialledDigits ? (const char*) pMCall->basicCallInformation->destination->dialledDigits->buf : "") : "")
		<< (pMCall->thirdPartyInformation ? BCDString(pMCall->thirdPartyInformation->thirdPartyNumber) : "")
		<< (pMCall->basicCallInformation->destination ? 
			(pMCall->basicCallInformation->destination->sMSDestinationNumber ? (const char*) pMCall->basicCallInformation->destination->sMSDestinationNumber->buf : "") : "");

	if( pMCall->thirdPartyInformation )
		if( pMCall->thirdPartyInformation->clirIndicator )
			otlStream << (short) *pMCall->thirdPartyInformation->clirIndicator;
		else
			otlStream << otl_null();
	else
		otlStream << otl_null();

	otlStream 
		<< (pMCall->basicCallInformation->destinationNetwork ? (const char*) pMCall->basicCallInformation->destinationNetwork->buf : "")
		<< pMCall->basicCallInformation->callEventStartTimeStamp->localTimeStamp->buf
		<< GetUTCOffset( *pMCall->basicCallInformation->callEventStartTimeStamp->utcTimeOffsetCode )
		<< *pMCall->basicCallInformation->totalCallEventDuration;

	if (pMCall->basicCallInformation->causeForTerm ) 
		otlStream << *pMCall->basicCallInformation->causeForTerm;
	else
		otlStream << otl_null();

	otlStream 
		<< GetRecordingEntity(*pMCall->locationInformation->networkLocation->recEntityCode, recEntityType)
		<< recEntityType;

	if (pMCall->locationInformation->networkLocation->locationArea )
		otlStream << *pMCall->locationInformation->networkLocation->locationArea;
	else
		otlStream << otl_null();
	
	if (pMCall->locationInformation->networkLocation->cellId )
		otlStream << *pMCall->locationInformation->networkLocation->cellId;
	else
		otlStream << otl_null();

	otlStream
		<< (pMCall->locationInformation->geographicalLocation ? ( pMCall->locationInformation->geographicalLocation->servingNetwork ?
			(const char*) pMCall->locationInformation->geographicalLocation->servingNetwork->buf : "") : "")
		<< (pMCall->equipmentIdentifier ?	( pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_imei ? BCDString( &pMCall->equipmentIdentifier->choice.imei ) : 
			(pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_esn ?	BCDString( &pMCall->equipmentIdentifier->choice.esn ) : "")) : "")
		<< (pMCall->basicCallInformation->rapFileSequenceNumber ? (const char*) pMCall->basicCallInformation->rapFileSequenceNumber->buf : "")
	;
	
	otlStream.flush();
	otlStream >> eventID;
	otlStream.close();
	
	// обработаем набор Basic Service Used
	long basicSvcID;
	char szChrInfo[500];
	for(int bs_ind=0; bs_ind < pMCall->basicServiceUsedList->list.count; bs_ind++)
	{
		// проверка наличия обязательных структур в Mobile Originated Call/Basic Service Used
		if(!pMCall->basicServiceUsedList->list.array[bs_ind]->basicService || !pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList ||
			!pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode)
		{
			log( LOG_ERROR, "Some mandatory structures are missing in Mobile Originated Call/Basic Service Used. Call number " + to_string( static_cast<unsigned long long> (index)));
			return TL_MISSINGSTRUCT;
		}
		
		otlStream.open( 1 , "INSERT INTO BILLING.TAP3_BASICSERVICE (SERVICE_ID,EVENT_ID,SERVICE_TYPE,SERVICE_CODE,CHR_TIME,CHR_UTCOFF,HSCSD) \
							VALUES (BILLING.TAP3EVENTID.NextVal, :hEventId /*bigint,in*/, :hServtype /*long,in*/, :hServcode /*char[5],in*/, \
							to_date(:hChrtime /*char[20],in*/,'yyyymmddhh24miss'), :hChr_utc /*char[10],in*/, :hHSCSD /*short,in*/) \
							returning SERVICE_ID into :hServiceId /*long,out*/", otlConnect);

		otlStream 
			<< eventID
			<< (long) (pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->present == BasicServiceCode_PR_bearerServiceCode)
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->present == BasicServiceCode_PR_bearerServiceCode ?
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->choice.bearerServiceCode.buf :
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->choice.teleServiceCode.buf )
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp ?
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp->localTimeStamp->buf : "")
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp ?
				GetUTCOffset( *pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp->utcTimeOffsetCode ) : "")
			<<	(pMCall->basicServiceUsedList->list.array[bs_ind]->hSCSDIndicator ? (short) 1 : (short) 0) ;

		otlStream.flush();
		otlStream >> basicSvcID;

		otlStream.close();
		long chrinfoRes;

		// обработаем набор Charge Information
		for(int chr_ind=0; chr_ind < pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList->list.count; chr_ind++)
		{
			sprintf(szChrInfo,"Call number %d. Basic Service number %d. Charge Information number %d",index,bs_ind,chr_ind);
			chrinfoRes=ProcessChrInfo(basicSvcID, pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList->list.array[chr_ind], szChrInfo);
			if(chrinfoRes<0) return chrinfoRes;
		}
	}
		
	return eventID;
}

//-----------------------------

long long ProcessTerminatedCall(long fileID, int index, const MobileTerminatedCall* pMCall)
{
	// проверка наличия обязательных структур в Mobile Terminated Call
	if(!pMCall->basicCallInformation || !pMCall->locationInformation || !pMCall->basicServiceUsedList)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Terminated Call. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}
	// проверка наличия обязательных структур в Mobile Terminated Call/MT Basic Call Information
	if(!pMCall->basicCallInformation->chargeableSubscriber || !pMCall->basicCallInformation->callEventStartTimeStamp ||
		!pMCall->basicCallInformation->totalCallEventDuration || !pMCall->basicCallInformation->callEventStartTimeStamp)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Terminated Call/MT Basic Call Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	// проверка наличия обязательных структур в Mobile Terminated Call/Location Information
	if(!pMCall->locationInformation->networkLocation  ||
		!pMCall->locationInformation->networkLocation->recEntityCode)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in Mobile Terminated Call/Location Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	long long eventID;
	string recEntityType;

	otl_nocommit_stream otlStream;
	
	otlStream.open( 1 /*stream buffer size in logical rows*/, 
		"insert into BILLING.TAP3_CALL (EVENT_ID,FILE_ID,RSN,ORIG_OR_TERM,IMSI,MSISDN,PARTY_NUMBER, SMS_PARTYNUMBER, \
			CLIR,PARTY_NETWORK,CALL_TIME,CALL_UTCOFF,DURATION,CAUSE_FOR_TERM,REC_ENTITY,REC_ENTITY_TYPE,\
			LOCATION_AREA,CELL_ID,SERVING_NETWORK,IMEI, RAP_FILE_SEQNUM ) VALUES (\
			BILLING.Origin_Seq.NextVal, :hFileID /* long,in */, :hIndex /* int,in */, :hOrigorterm /*short,in*/, :hImsi /* char[20],in */, :hMsisdn  /* char[20],in */, \
			:hCallingnum  /* char[40],in */, :hSMSOrigin /* char[40],in */, \
			:hClir  /* short,in */, :hPartynetw  /* char[20],in */ , \
			to_date(:hCalltime  /* char[20],in */,'yyyymmddhh24miss'), :hCall_utc /* char[10],in */,:hDuration  /* long,in */ ,:hCause  /* long,in */,\
			:hRecentity /* char[30],in */, :hRecentityType /* char[10],in */, :hLocarea /* long,in */, :hCellid /* long,in */, \
			:hServnetw /* char[20],in */, :hImei /* char[30],in */, :hRAPSeqnum /* char[10],in */) returning EVENT_ID into :hEventId /*bigint,out*/", otlConnect);
	
	otlStream 
		<< fileID
		<< index 
		<< (short) 0
		<< BCDString( pMCall->basicCallInformation->chargeableSubscriber->choice.simChargeableSubscriber.imsi )
		<< BCDString( pMCall->basicCallInformation->chargeableSubscriber->choice.simChargeableSubscriber.msisdn )
		<< (pMCall->basicCallInformation->callOriginator ? 
				(pMCall->basicCallInformation->callOriginator->callingNumber ? BCDString(pMCall->basicCallInformation->callOriginator->callingNumber) : "") : "")
		<< (pMCall->basicCallInformation->callOriginator ? 
				(pMCall->basicCallInformation->callOriginator->sMSOriginator ? (const char*) pMCall->basicCallInformation->callOriginator->sMSOriginator->buf : "") : "");

	if (pMCall->basicCallInformation->callOriginator ) 
		if (pMCall->basicCallInformation->callOriginator->clirIndicator)
			otlStream << (short) *pMCall->basicCallInformation->callOriginator->clirIndicator;
		else
			otlStream << otl_null();
	else
		otlStream << otl_null();
	
	otlStream 
		<< (pMCall->basicCallInformation->originatingNetwork ? (const char*) pMCall->basicCallInformation->originatingNetwork->buf : "")
		<< pMCall->basicCallInformation->callEventStartTimeStamp->localTimeStamp->buf
		<< GetUTCOffset( *pMCall->basicCallInformation->callEventStartTimeStamp->utcTimeOffsetCode )
		<< *pMCall->basicCallInformation->totalCallEventDuration;

	if (pMCall->basicCallInformation->causeForTerm )
		otlStream << *pMCall->basicCallInformation->causeForTerm ;
	else
		otlStream << otl_null();

	otlStream 
		<< GetRecordingEntity(*pMCall->locationInformation->networkLocation->recEntityCode, recEntityType)
		<< recEntityType;

	if (pMCall->locationInformation->networkLocation->locationArea )
		otlStream << *pMCall->locationInformation->networkLocation->locationArea ;
	else
		otlStream << otl_null();

	if( pMCall->locationInformation->networkLocation->cellId )
		otlStream << *pMCall->locationInformation->networkLocation->cellId ;
	else
		otlStream << otl_null();

	otlStream 
		<< (pMCall->locationInformation->geographicalLocation ? ( pMCall->locationInformation->geographicalLocation->servingNetwork ?
			(const char*) pMCall->locationInformation->geographicalLocation->servingNetwork->buf : "") : "")
		<< (pMCall->equipmentIdentifier ?	( pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_imei ? BCDString( &pMCall->equipmentIdentifier->choice.imei ) : 
			(pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_esn ?	BCDString( &pMCall->equipmentIdentifier->choice.esn ) : "")) : "")
		<< (pMCall->basicCallInformation->rapFileSequenceNumber ? (const char*) pMCall->basicCallInformation->rapFileSequenceNumber->buf : "")
	;
	
	otlStream.flush();
	otlStream >> eventID;
	otlStream.close();
	
	// обработаем набор Basic Service Used
	long basicSvcID;
	char szChrInfo[500];
	for(int bs_ind=0; bs_ind < pMCall->basicServiceUsedList->list.count; bs_ind++)
	{
		// проверка наличия обязательных структур в Mobile Originated Call/Basic Service Used
		if(!pMCall->basicServiceUsedList->list.array[bs_ind]->basicService || !pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList ||
			!pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode)
		{
			log( LOG_ERROR, "Some mandatory structures are missing in Mobile Terminated Call/Basic Service Used. Call number " + to_string( static_cast<unsigned long long> (index)));
			return TL_MISSINGSTRUCT;
		}
		
		otlStream.open( 1 , "INSERT INTO BILLING.TAP3_BASICSERVICE (SERVICE_ID,EVENT_ID,SERVICE_TYPE,SERVICE_CODE,CHR_TIME,CHR_UTCOFF,HSCSD) \
							VALUES (BILLING.TAP3EVENTID.NextVal, :hEventId /*bigint,in*/, :hServtype /*long,in*/, :hServcode /*char[5],in*/, \
							to_date(:hChrtime /*char[20],in*/,'yyyymmddhh24miss'), :hChr_utc /*char[10],in*/, :hHSCSD /*short,in*/) \
							returning SERVICE_ID into :hServiceId /*long,out*/", otlConnect);

		otlStream 
			<< eventID
			<< (long) (pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->present == BasicServiceCode_PR_bearerServiceCode)
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->present == BasicServiceCode_PR_bearerServiceCode ?
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->choice.bearerServiceCode.buf :
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->basicService->serviceCode->choice.teleServiceCode.buf )
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp ?
				(const char*)pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp->localTimeStamp->buf : "")
			<< (pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp ?
				GetUTCOffset( *pMCall->basicServiceUsedList->list.array[bs_ind]->chargingTimeStamp->utcTimeOffsetCode ) : "")
			<<	(pMCall->basicServiceUsedList->list.array[bs_ind]->hSCSDIndicator ? (short) 1 : (short) 0) ;

		otlStream.flush();
		otlStream >> basicSvcID;

		otlStream.close();
		long chrinfoRes;

		// обработаем набор Charge Information
		for(int chr_ind=0; chr_ind < pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList->list.count; chr_ind++)
		{
			sprintf(szChrInfo,"Номер звонка %d\nНомер Basic Service %d\nНомер Charge Information %d",index,bs_ind,chr_ind);
			chrinfoRes=ProcessChrInfo(basicSvcID, pMCall->basicServiceUsedList->list.array[bs_ind]->chargeInformationList->list.array[chr_ind], szChrInfo);
			if(chrinfoRes<0) return chrinfoRes;
		}
	}
		
	return eventID;
}


//-----------------------------

long long ProcessGPRSCall(long fileID, int index, const GprsCall* pMCall)
{
	// проверка наличия обязательных структур в Mobile Terminated Call
	if(!pMCall->gprsBasicCallInformation|| !pMCall->gprsLocationInformation || !pMCall->gprsServiceUsed)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in GPRS Call. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}
	// проверка наличия обязательных структур в Mobile Terminated Call/MT Basic Call Information
	if(!pMCall->gprsBasicCallInformation->gprsChargeableSubscriber || !pMCall->gprsBasicCallInformation->gprsDestination || !pMCall->gprsBasicCallInformation->callEventStartTimeStamp ||
		!pMCall->gprsBasicCallInformation->totalCallEventDuration || !pMCall->gprsBasicCallInformation->chargingId)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in GPRS Call/GPRS Basic Call Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	// проверка наличия обязательных структур в Mobile Terminated Call/Location Information
	if(!pMCall->gprsLocationInformation->gprsNetworkLocation  || !pMCall->gprsLocationInformation->gprsNetworkLocation->recEntity)
	{
		log( LOG_ERROR, "Some mandatory structures are missing in GPRS Call/GPRS Location Information. Call number " + to_string( static_cast<unsigned long long> (index)));
		return TL_MISSINGSTRUCT;
	}

	long long eventID;
	string recEntityType;

	otl_nocommit_stream otlStream;	
	
	otlStream.open( 1 /*stream buffer size in logical rows*/, 
		"INSERT INTO BILLING.TAP3_GPRSCALL (\
			EVENT_ID, \
			FILE_ID, \
			RSN, \
			IMSI, \
			MSISDN, \
			PDP_ADDRESS, \
			APN_NI, \
			APN_OI, \
			CALL_TIME, \
			CALL_UTCOFF, \
			DURATION, \
			CAUSE_FOR_TERM, \
			PARTIAL_TYPE, \
			PDP_START_TIME, \
			PDP_START_UTCOFF, \
			CHARGING_ID, \
			REC_ENTITY, \
			REC_ENTITY_TYPE, \
			REC_ENTITY2, \
			REC_ENTITY2_TYPE, \
			LOCATION_AREA, \
			CELL_ID, \
			SERVING_NETWORK, \
			IMEI, \
			RAP_FILE_SEQNUM, \
			VOLUME_INCOMING, \
			VOLUME_OUTGOING \
				) VALUES (\
			BILLING.Origin_Seq.NextVal, \
			:hFileid /* long,in */, \
			:hIndex /* int,in */, \
			:hImsi /* char[30],in */, \
			:hMsisdn /* char[30],in */, \
			:hPdpaddr /* char[50],in */, \
			:hApnni /* char[70],in */, \
			:hApnoi /* char[70],in */, \
			to_date(:hCalltime  /* char[20],in */,'yyyymmddhh24miss'), \
			:hCall_utc /* char[20],in */, \
			:hDuration /* long,in */, \
			:hCause /* long,in */,\
			:hPartial /* char[5],in */, \
			to_date(:hPdpstart /* char[20],in */,'yyyymmddhh24miss'),\
			:hPdp_utc /* char[10],in */, \
			:hChargid /* bigint,in */, \
			:hRecentity /* char[50],in */,\
			:hRecentityType  /* char[10],in */,\
			:hRecentity2 /* char[50],in */,\
			:hRecentity2Type  /* char[10],in */,\
			:hLocarea /* long,in */, \
			:hCellid /* long,in */, \
			:hServingNet /* char[20],in */,\
			:hImei /* char[30],in */, \
			:RapFileSN /* char[10],in */, \
			:VolIncoming /* bigint,in */,\
			:VolOutgoing /* bigint,in */) \
			returning EVENT_ID into :hEventId /*bigint,out*/", otlConnect);
	
	otlStream 
		<< fileID
		<< index 
		<< BCDString( pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->chargeableSubscriber->choice.simChargeableSubscriber.imsi )
		<< (pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->chargeableSubscriber->choice.simChargeableSubscriber.msisdn ? 
				BCDString( pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->chargeableSubscriber->choice.simChargeableSubscriber.msisdn ) :
					(pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->networkAccessIdentifier ? 
						(const char*) pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->networkAccessIdentifier->buf : ""))
		<< (pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->pdpAddress ? 
			(const char*) pMCall->gprsBasicCallInformation->gprsChargeableSubscriber->pdpAddress->buf : "")
		<< (pMCall->gprsBasicCallInformation->gprsDestination->accessPointNameNI ? 
			(const char*) pMCall->gprsBasicCallInformation->gprsDestination->accessPointNameNI->buf : "")
		<< (pMCall->gprsBasicCallInformation->gprsDestination->accessPointNameOI ? 
			(const char*) pMCall->gprsBasicCallInformation->gprsDestination->accessPointNameOI->buf : "")
		<< pMCall->gprsBasicCallInformation->callEventStartTimeStamp->localTimeStamp->buf 
		<< GetUTCOffset( *pMCall->gprsBasicCallInformation->callEventStartTimeStamp->utcTimeOffsetCode )
		<< *pMCall->gprsBasicCallInformation->totalCallEventDuration;

	if(pMCall->gprsBasicCallInformation->causeForTerm )
		otlStream << *pMCall->gprsBasicCallInformation->causeForTerm;
	else
		otlStream << otl_null();

	otlStream
		<< (pMCall->gprsBasicCallInformation->partialTypeIndicator ? (const char*)pMCall->gprsBasicCallInformation->partialTypeIndicator->buf : "")
		<< (pMCall->gprsBasicCallInformation->pDPContextStartTimestamp ? (const char*)pMCall->gprsBasicCallInformation->pDPContextStartTimestamp->localTimeStamp->buf : "")
		<< (pMCall->gprsBasicCallInformation->pDPContextStartTimestamp ? GetUTCOffset(*pMCall->gprsBasicCallInformation->pDPContextStartTimestamp->utcTimeOffsetCode) : "")
		<< OctetStr2Int64(*pMCall->gprsBasicCallInformation->chargingId);
	if (pMCall->gprsLocationInformation->gprsNetworkLocation->recEntity->list.count > 0) {
		// first recording entity
		otlStream
			<< GetRecordingEntity(*pMCall->gprsLocationInformation->gprsNetworkLocation->recEntity->list.array[0], recEntityType)
			<< recEntityType;
	}
	else {
		otlStream
			<< ""
			<< "";
	}

	if (pMCall->gprsLocationInformation->gprsNetworkLocation->recEntity->list.count > 1) {
		// second recording entity
		otlStream
			<< GetRecordingEntity(*pMCall->gprsLocationInformation->gprsNetworkLocation->recEntity->list.array[1], recEntityType)
			<< recEntityType;
	}
	else {
		otlStream
			<< ""
			<< "";
	}

	if( pMCall->gprsLocationInformation->gprsNetworkLocation->locationArea )
		otlStream << *pMCall->gprsLocationInformation->gprsNetworkLocation->locationArea ;
	else
		otlStream << otl_null();

	if( pMCall->gprsLocationInformation->gprsNetworkLocation->locationArea )
		cout << *pMCall->gprsLocationInformation->gprsNetworkLocation->locationArea << endl;
	else
		cout << otl_null() << endl;

	if (pMCall->gprsLocationInformation->gprsNetworkLocation->cellId )
		otlStream << *pMCall->gprsLocationInformation->gprsNetworkLocation->cellId ;
	else
		otlStream << otl_null();

	if (pMCall->gprsLocationInformation->gprsNetworkLocation->cellId )
		cout << *pMCall->gprsLocationInformation->gprsNetworkLocation->cellId << endl;
	else
		cout << otl_null() << endl;

	otlStream 
		<< (pMCall->gprsLocationInformation->geographicalLocation ? ( pMCall->gprsLocationInformation->geographicalLocation->servingNetwork ?
			(const char*) pMCall->gprsLocationInformation->geographicalLocation->servingNetwork->buf : "") : "")
		<< (pMCall->equipmentIdentifier ?	( pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_imei ? BCDString( &pMCall->equipmentIdentifier->choice.imei ) : 
			(pMCall->equipmentIdentifier->present == ImeiOrEsn_PR_esn ?	BCDString( &pMCall->equipmentIdentifier->choice.esn ) : "")) : "")
		<< (pMCall->gprsBasicCallInformation->rapFileSequenceNumber ? (const char*) pMCall->gprsBasicCallInformation->rapFileSequenceNumber->buf : "")
		<< OctetStr2Int64 (*pMCall->gprsServiceUsed->dataVolumeIncoming )
		<< OctetStr2Int64 (*pMCall->gprsServiceUsed->dataVolumeOutgoing ) ;

	otlStream.flush();
	otlStream >> eventID;
	otlStream.close();

	char szChrInfo[500];
	long chrinfoRes;
	// обработаем набор Charge Information
	for(int chr_ind=0; chr_ind < pMCall->gprsServiceUsed->chargeInformationList->list.count; chr_ind++)
	{
		sprintf(szChrInfo,"Номер звонка %d\nНомер Charge Information %d",index, chr_ind);
		chrinfoRes=ProcessChrInfo(eventID, pMCall->gprsServiceUsed->chargeInformationList->list.array[chr_ind], szChrInfo);
		if(chrinfoRes<0) return chrinfoRes;
	}

	return eventID;
}

//-----------------------------
void Finalize(bool bSuccess = false)
{
	if( dataInterchange )     
		ASN_STRUCT_FREE(asn_DEF_DataInterChange, dataInterchange);
	if (returnBatch)
		ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	if (acknowledgement)
		ASN_STRUCT_FREE(asn_DEF_Acknowledgement, acknowledgement);

	if( otlConnect.connected ) {
		if( bSuccess )
			otlConnect.commit();
		else
			otlConnect.rollback();
		otlConnect.logoff();
	}

	/*if( otlLogConnect.connected ) {
		log( bSuccess? LOG_INFO : LOG_ERROR, bSuccess ? "---------- TAP3 loader finished successfully --------------" : "---------- TAP3 loader finished with errors --------------");*/
		otlLogConnect.commit();
		otlLogConnect.logoff();
	//}

	if(ofsLog.is_open()) ofsLog.close();
	if (buffer ) delete [] buffer;
}
//------------------------------

int LoadTAPFileToDB( unsigned char* buffer, long dataLen, long fileID, long roamingHubID, bool bPrintOnly ) 
{
	int index=0;
	try {
		asn_dec_rval_t rval;
	
		rval = ber_decode(0, &asn_DEF_DataInterChange, (void**) &dataInterchange, buffer, dataLen);

		if(rval.code != RC_OK) {
			log( LOG_ERROR, string("Error while decoding ASN file. Error code ") + to_string( static_cast<unsigned long long> (rval.code)));
			Finalize();
			return TL_DECODEERROR;
		}

		if( bPrintOnly ) {
			char* printName = new char[ strlen(pShortName)+5 ];
			sprintf(printName, "%s.txt", pShortName);
			FILE* fFileContents = fopen (printName, "w");
			if ( fFileContents ) {
				asn_fprint(fFileContents, &asn_DEF_DataInterChange, dataInterchange);
				fclose(fFileContents);
			}
			else
			{
				printf("Unable to open output file %s\n", printName);
			}
			delete [] printName;
			printf("---- File contents printed to output file. Exiting. -------");
			return TL_OK;
		}

		otl_nocommit_stream otlStream;
		TAPValidator tapValidator(otlConnect, config);
		TAPValidationResult validationRes = tapValidator.Validate(dataInterchange, roamingHubID);
		if (validationRes == VALIDATION_IMPOSSIBLE) {
			log(LOG_ERROR, "Unable to validate TAP file. File is not loaded.");
			return TL_TAP_NOT_VALIDATED;
		}
		else if (validationRes == FILE_DUPLICATION) {
			// no uploading needed
			return TL_OK;
		}
		
		if (dataInterchange->present == DataInterChange_PR_notification) {
			// Processing Notification (empty TAP file, i.e. with no call records)
			otlStream.open( 1 /*stream buffer size in logical rows*/, 
				"insert into BILLING.TAP3_FILE (FILE_ID, ROAMINGHUB_ID, FILENAME, SENDER, RECIPIENT, SEQUENCE_NUMBER , CREATION_STAMP, CREATION_UTCOFF,"
				"CUTOFF_STAMP, CUTOFF_UTCOFF, AVAILABLE_STAMP, AVAILABLE_UTCOFF, LOAD_TIME, NOTIFICATION, TAP_VERSION, TAP_RELEASE, "
				"FILE_TYPE_INDICATOR, STATUS, CANCEL_RAP_FILE_SEQNUM, CANCEL_RAP_FILE_ID) "
				"values ("
				":hfileid /*long,in*/, :roamhubid /*long,in*/, :filename/*char[255],in*/, :sender/*char[20],in*/, :recipient/*char[20],in*/, :seq_num/*char[10],in*/,"
				"to_date(:creation_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :creation_utcoff /* char[10],in*/,"
				"to_date(:cutoff_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :cutoff_utcoff /* char[10],in*/,"
				"to_date(:available_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :available_utcoff /* char[10],in*/,"
				"sysdate, 1 /*notification*/, "
				":tapVer /*long,in*/, :specif /*long,in*/, :filetype /*char[5],in*/, :status /*long,in*/,"
				":cancel_rap_file_seqnum /*char[10],in*/, :cancel_rap_file_id /*long,in*/)", otlConnect); 
			otlStream
				<< fileID
				<< roamingHubID
				<< pShortName 
				<< (char*) dataInterchange->choice.notification.sender->buf 
				<< (char*) dataInterchange->choice.notification.recipient->buf 
				<< dataInterchange->choice.notification.fileSequenceNumber->buf
				<< (dataInterchange->choice.notification.fileCreationTimeStamp ? (const char*) dataInterchange->choice.notification.fileCreationTimeStamp->localTimeStamp->buf : "")
				<< (dataInterchange->choice.notification.fileCreationTimeStamp ? (const char*)dataInterchange->choice.notification.fileCreationTimeStamp->utcTimeOffset->buf : "")
				<< dataInterchange->choice.notification.transferCutOffTimeStamp->localTimeStamp->buf
				<< dataInterchange->choice.notification.transferCutOffTimeStamp->utcTimeOffset->buf
				<< dataInterchange->choice.notification.fileAvailableTimeStamp->localTimeStamp->buf
				<< dataInterchange->choice.notification.fileAvailableTimeStamp->utcTimeOffset->buf
				<< *dataInterchange->choice.notification.specificationVersionNumber
				<< *dataInterchange->choice.notification.releaseVersionNumber
				<< (dataInterchange->choice.notification.fileTypeIndicator ? (const char*) dataInterchange->choice.notification.fileTypeIndicator->buf : "");
			if (validationRes == FATAL_ERROR) 
				otlStream 
					<< (long) INFILE_STATUS_FATAL
					<< tapValidator.GetRapSequenceNum()
					<< tapValidator.GetRapFileID();
			else
				otlStream 
					<< (long) INFILE_STATUS_NEW
					<< otl_null()
					<< otl_null();
			
			otlStream.flush();
			otlStream.close();

			return TL_OK;
		}

		// Processing Transfer Batch
		otlStream.open( 1 /*stream buffer size in logical rows*/, 
			"insert into BILLING.TAP3_FILE (FILE_ID, ROAMINGHUB_ID, FILENAME, SENDER, RECIPIENT, SEQUENCE_NUMBER , CREATION_STAMP, CREATION_UTCOFF,\
			CUTOFF_STAMP, CUTOFF_UTCOFF, AVAILABLE_STAMP, AVAILABLE_UTCOFF, LOCAL_CURRENCY, LOAD_TIME, EARLIEST_TIME, EARLIEST_UTCOFF, \
			LATEST_TIME, LATEST_UTCOFF, EVENT_COUNT, TOTAL_CHARGE, TOTAL_TAX, TOTAL_DISCOUNT, NOTIFICATION, STATUS, TAP_VERSION, TAP_RELEASE, "
			"FILE_TYPE_INDICATOR, TAP_DECIMAL_PLACES, CANCEL_RAP_FILE_SEQNUM, CANCEL_RAP_FILE_ID) \
			values (\
			  :hfileid /*long,in*/, :roamhubid /*long,in*/, :filename/*char[255],in*/, :sender/*char[20],in*/, :recipient/*char[20],in*/, :seq_num/*char[10],in*/,\
			  to_date(:creation_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :creation_utcoff /* char[10],in */,\
			  to_date(:cutoff_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :cutoff_utcoff /* char[10],in */,\
			  to_date(:available_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :available_utcoff /* char[10],in */,\
			  :local_currency /* char[255],in */, sysdate, to_date(:earliest /*char[20],in*/, 'yyyymmddhh24miss'), :earliest_utcoff /* char[10],in */,\
			  to_date(:latest /*char[20],in*/, 'yyyymmddhh24miss'), :latest_utcoff /* char[10],in */, :eventcount /* long,in */, :totalchr /* double,in */, \
			  :total_tax /*double,in*/, :total_discount /*double,in*/, 0 /*notification*/, :status /*long,in*/, :tapVer /*long,in*/, :specif /*long,in*/, "
			  ":filetype /*char[5],in*/, :tapDecimalPlaces /*long,in*/, :cancel_rap_file_seqnum /*char[10],in*/, :cancel_rap_file_id /*long,in*/) ", otlConnect);
		otlStream 
			<< fileID
			<< roamingHubID
			<< pShortName 
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->sender ?
				 (char*) dataInterchange->choice.transferBatch.batchControlInfo->sender->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->recipient ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->recipient->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->fileSequenceNumber ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->fileSequenceNumber->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->fileCreationTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->fileCreationTimeStamp->localTimeStamp->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->fileCreationTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->fileCreationTimeStamp->utcTimeOffset->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->transferCutOffTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->transferCutOffTimeStamp->localTimeStamp->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->transferCutOffTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->transferCutOffTimeStamp->utcTimeOffset->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->fileAvailableTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf : "" )
			<< ( dataInterchange->choice.transferBatch.batchControlInfo->fileAvailableTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.batchControlInfo->fileAvailableTimeStamp->utcTimeOffset->buf : "" )
			<< ( dataInterchange->choice.transferBatch.accountingInfo->localCurrency ?
				(char*) dataInterchange->choice.transferBatch.accountingInfo->localCurrency->buf : "" )
			<< ( dataInterchange->choice.transferBatch.auditControlInfo->earliestCallTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.auditControlInfo->earliestCallTimeStamp->localTimeStamp->buf : "" )
			<< ( dataInterchange->choice.transferBatch.auditControlInfo->earliestCallTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.auditControlInfo->earliestCallTimeStamp->utcTimeOffset->buf : "" )
			<< ( dataInterchange->choice.transferBatch.auditControlInfo->latestCallTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.auditControlInfo->latestCallTimeStamp->localTimeStamp->buf : "" )
			<< ( dataInterchange->choice.transferBatch.auditControlInfo->latestCallTimeStamp ?
				(char*) dataInterchange->choice.transferBatch.auditControlInfo->latestCallTimeStamp->utcTimeOffset->buf : "" );
		if (dataInterchange->choice.transferBatch.auditControlInfo->callEventDetailsCount)
			otlStream << *dataInterchange->choice.transferBatch.auditControlInfo->callEventDetailsCount;
		else
			otlStream << otl_null();
		if (dataInterchange->choice.transferBatch.auditControlInfo->totalCharge)
			otlStream << OctetStr2Int64(*dataInterchange->choice.transferBatch.auditControlInfo->totalCharge) / GetTAPPower();
		else
			otlStream << otl_null();
		if (dataInterchange->choice.transferBatch.auditControlInfo->totalTaxValue)
			otlStream << OctetStr2Int64(*dataInterchange->choice.transferBatch.auditControlInfo->totalTaxValue) / GetTAPPower();
		else
			otlStream << otl_null();
		if (dataInterchange->choice.transferBatch.auditControlInfo->totalDiscountValue)
			otlStream << OctetStr2Int64(*dataInterchange->choice.transferBatch.auditControlInfo->totalDiscountValue) / GetTAPPower();
		else
			otlStream << otl_null();

		if (validationRes == FATAL_ERROR) 
			otlStream << (long) INFILE_STATUS_FATAL;
		else
			otlStream << (long) INFILE_STATUS_NEW;
			
		if (dataInterchange->choice.transferBatch.batchControlInfo->specificationVersionNumber)
			otlStream << *dataInterchange->choice.transferBatch.batchControlInfo->specificationVersionNumber;
		else
			otlStream << otl_null();
		if (dataInterchange->choice.transferBatch.batchControlInfo->releaseVersionNumber)
			otlStream << *dataInterchange->choice.transferBatch.batchControlInfo->releaseVersionNumber;
		else
			otlStream << otl_null();

		otlStream << ( dataInterchange->choice.transferBatch.batchControlInfo->fileTypeIndicator ? 
			(const char*) dataInterchange->choice.transferBatch.batchControlInfo->fileTypeIndicator->buf : "" );

		if (dataInterchange->choice.transferBatch.accountingInfo->tapDecimalPlaces)
			otlStream << *dataInterchange->choice.transferBatch.accountingInfo->tapDecimalPlaces;
		else
			otlStream << otl_null();

		if (validationRes == FATAL_ERROR)
			otlStream
				<< tapValidator.GetRapSequenceNum()
				<< tapValidator.GetRapFileID();
		else
			otlStream
				<< otl_null()
				<< otl_null();

		otlStream.flush();
		otlStream.close();
		
		if (validationRes == FATAL_ERROR)
			// Finish processing here. No call records are uploaded for Fatal Error TAP files
			return TL_OK;
		
		long long eventID;
		
		// обработка звонковых записей
		for(index=1; index <= dataInterchange->choice.transferBatch.callEventDetails->list.count; index++)
		{
			switch( dataInterchange->choice.transferBatch.callEventDetails->list.array[index-1]->present) {
			case CallEventDetail_PR_mobileOriginatedCall:
				if ((eventID = ProcessOriginatedCall(fileID, index, &dataInterchange->choice.transferBatch.callEventDetails->list.array[index - 1]->choice.mobileOriginatedCall)) < 0)
				{
					return (long) eventID;
				}
				
				break;
			case CallEventDetail_PR_mobileTerminatedCall:
				if ((eventID = ProcessTerminatedCall(fileID, index, &dataInterchange->choice.transferBatch.callEventDetails->list.array[index - 1]->choice.mobileTerminatedCall)) < 0)
				{
					return (long)eventID;
				}
				
				break;
			case CallEventDetail_PR_supplServiceEvent:
				// at this time we just ignore it
				break;

			case CallEventDetail_PR_gprsCall:
				if ((eventID = ProcessGPRSCall(fileID, index, &dataInterchange->choice.transferBatch.callEventDetails->list.array[index - 1]->choice.gprsCall)) < 0)
				{
					return (long) eventID;
				}
				
				break;
			default:
				if (!debugMode) {
					log(pShortName, LOG_ERROR, string("No handler for call event detail type=") + to_string( static_cast<unsigned long long> (dataInterchange->choice.transferBatch.callEventDetails->list.array[index-1]->present)) +
						string(". Call number=") + to_string(static_cast<unsigned long long> (index)));
					return TL_NEWCOMPONENT;
				}
			}
			
		}

		// TODO: disable, move to TAP Validator
		// обработка Audit Control Info
		if(!dataInterchange->choice.transferBatch.auditControlInfo->totalCharge || !dataInterchange->choice.transferBatch.auditControlInfo->callEventDetailsCount)
		{
			log(pShortName, LOG_ERROR, "Some mandatory structures are missing in Transfer Batch/Audit Control Information");
			return TL_WRONGCODE;
		}
	
		if(index-1 != *dataInterchange->choice.transferBatch.auditControlInfo->callEventDetailsCount)
		{
			log(pShortName, LOG_ERROR, "Calculated call event count (" + to_string(static_cast<unsigned long long> (index))+") differs from one in Audit Control Information ( " +
				to_string( static_cast<unsigned long long> (*dataInterchange->choice.transferBatch.auditControlInfo->callEventDetailsCount)) +
				").");
			return TL_AUDITFAULT;
		}

		if(!debugMode && totalCharge != OctetStr2Int64(*dataInterchange->choice.transferBatch.auditControlInfo->totalCharge))
		{
			log(pShortName, LOG_ERROR, "Calculated total charge (" + to_string(static_cast<unsigned long double> (totalCharge / GetTAPPower())) + ") differs from Audit Control Information/TotalCharge (" +
				to_string( static_cast<long double> (OctetStr2Int64(*dataInterchange->choice.transferBatch.auditControlInfo->totalCharge)/GetTAPPower())) + ").");
			return TL_AUDITFAULT;
		}

		return TL_OK;
	}
	catch (otl_exception &otlEx) {
		otlConnect.rollback();
		log(pShortName,  LOG_ERROR, "DB error while processing CDR records:");
		log(pShortName,  LOG_ERROR, (char*) otlEx.msg);
		if( strlen(otlEx.stm_text) > 0 )
			log(pShortName,  LOG_ERROR, (char*) otlEx.stm_text ); // log SQL that caused the error
		if( strlen(otlEx.var_info) > 0 )
			log(pShortName,  LOG_ERROR, (char*) otlEx.var_info ); // log the variable that caused the error
		return TL_ORACLEERROR;
	}
	
	catch(char* pMess)
	{
		log(pShortName, LOG_ERROR, string("Exception caught: ") + string(pMess) + string(". Call number=") + to_string( static_cast<unsigned long long> (index)));
		return TL_WRONGCODE;
	}
	return TL_OK;
}

//-----------------------------------------------------

int LoadRAPStopOrMissingInfo(long fileID, string stopLastSeqNum, string missStartSeqNum, string missEndSeqNum, OperatorSpecList* pOperSpecList)
{
	string operSpecInfo;
	if (pOperSpecList) {
		// concatenate operator specific info before loading
		for (int i = 0; i < pOperSpecList->list.count; i++) {
			if (operSpecInfo.length() > 0)
				operSpecInfo += "\r\n";
			operSpecInfo += (char*)pOperSpecList->list.array[i]->buf;
		}
		if (operSpecInfo.length() > 1024)
			operSpecInfo = operSpecInfo.substr(0, 1024);
	}

	otl_nocommit_stream otlStream;

	otlStream.open(1 /*stream buffer size in logical rows*/,
		"insert into BILLING.RAP_STOP_OR_MISSING (FILE_ID, STOP_LAST_SEQ_NUM, MISS_START_SEQ_NUM, MISS_END_SEQ_NUM, OPERATOR_SPEC_INFO) \
					values (:hfileid /*long*/, :stop_last_seqnum/*char[10]*/, :miss_start/*char[10]*/, :miss_end/*char[10]*/, \
					:oper_spec_info /*char[1024]*/)", otlConnect);

	otlStream
		<< fileID
		<< stopLastSeqNum
		<< missStartSeqNum
		<< missEndSeqNum
		<< operSpecInfo;

	return TL_OK;
}

//------------------------------------------------

int LoadRAPErrorDetailList(const ErrorDetailList_t* pErrDetailList, long returnID)
{
	otl_nocommit_stream otlStream;

	for (int detail = 0; detail < pErrDetailList->list.count; detail++) {
		long detailID;
		otlStream.open(1 /*stream buffer size in logical rows*/,
			"insert into BILLING.RAP_ERROR_DETAIL (DETAIL_ID, RETURN_ID, ERROR_CODE, ITEM_OFFSET ) "
			"values (Billing.TAP3EventID.NextVal, :return_id /*long,in*/, :error_code/*long,in*/, :item_offset/*long,in*/) "
			"returning DETAIL_ID into :detail_id /*long,out*/", otlConnect);

		otlStream
			<< returnID
			<< pErrDetailList->list.array[detail]->errorCode;

		if (pErrDetailList->list.array[detail]->itemOffset)
			otlStream << *pErrDetailList->list.array[detail]->itemOffset;
		else
			otlStream << otl_null();

		otlStream.flush();
		otlStream >> detailID;
		otlStream.close();

		// context data loading to DB
		asn_TYPE_descriptor_t * type_descriptor = &asn_DEF_DataInterChange;
		for (int context = 0; context < pErrDetailList->list.array[detail]->errorContext->list.count; context++) {
			// Parsing Path Item ID which means ASN tag of TAP file
			ber_tlv_tag_t tlv_tag = (pErrDetailList->list.array[detail]->errorContext->list.array[context]->pathItemId << 2) | ASN_TAG_CLASS_APPLICATION;

			// while elements[0.tag == -1 fall just through structures 
			while (type_descriptor->elements[0].tag == (ber_tlv_tag_t)-1)
				type_descriptor = type_descriptor->elements[0].type;

			// linear search for tlv_tag in elements array
			asn_TYPE_member_t *elm = NULL;
			for (int n = 0; n < type_descriptor->elements_count; n++) {
				if (BER_TAGS_EQUAL(tlv_tag, type_descriptor->elements[n].tag)) {
					elm = &type_descriptor->elements[n];
					break;
				}
			}

			if (!elm) {
				log(LOG_ERROR, "Error parsing Path Item IDs at error context #" + to_string((long long)context + 1) + " for detail #" + to_string((long long)detail + 1));
				log(LOG_ERROR, "ASN Tag " + to_string((long long) pErrDetailList->list.array[detail]->errorContext->list.array[context]->pathItemId)
					+ " not found in ASN type " + type_descriptor->name);

				return TL_DECODEERROR;
			}

			type_descriptor = elm->type;

			otl_nocommit_stream otlStreamCtx;
			otlStreamCtx.open(1 /*stream buffer size in logical rows*/,
				"insert into BILLING.RAP_ERROR_CONTEXT (DETAIL_ID, CONTEXT_SEQNUM, PATH_ITEM_ID, ITEM_LEVEL, ITEM_NAME, ITEM_OCCURENCE ) \
						values (:detail_id /*long,in*/, :ctx_seqnum /*long,in*/, :path_item /*long,in*/, :item_level/*long,in*/, \
								:item_name/*char[100],in*/, :item_occur/*long,in*/)", otlConnect);
			otlStreamCtx
				<< detailID
				<< (long)context + 1
				<< pErrDetailList->list.array[detail]->errorContext->list.array[context]->pathItemId
				<< pErrDetailList->list.array[detail]->errorContext->list.array[context]->itemLevel
				<< type_descriptor->name;

			if (pErrDetailList->list.array[detail]->errorContext->list.array[context]->itemOccurrence)
				otlStreamCtx << *pErrDetailList->list.array[detail]->errorContext->list.array[context]->itemOccurrence;
			else
				otlStreamCtx << otl_null();

			otlStreamCtx.close();
		}
	}

	return TL_OK;
}

//------------------------------------------------

int LoadRAPFatalReturn(long fileID, const FatalReturn& fatalReturn)
{
	string errorType;
	ErrorDetailList_t* pErrDetailList;
	if (fatalReturn.accountingInfoError) {
		errorType = "Accounting Info";
		pErrDetailList = &fatalReturn.accountingInfoError->errorDetail;
	}
	else if (fatalReturn.auditControlInfoError) {
		errorType = "Audit Control Info";
		pErrDetailList = &fatalReturn.auditControlInfoError->errorDetail;
	}
	else if (fatalReturn.batchControlError) {
		errorType = "Batch Control Info";
		pErrDetailList = &fatalReturn.batchControlError->errorDetail;
	}
	else if (fatalReturn.messageDescriptionError) {
		errorType = "Message Description";
		pErrDetailList = &fatalReturn.messageDescriptionError->errorDetail;
	}
	else if (fatalReturn.networkInfoError) {
		errorType = "Network Info";
		pErrDetailList = &fatalReturn.networkInfoError->errorDetail;
	}
	else if (fatalReturn.notificationError) {
		errorType = "Notification";
		pErrDetailList = &fatalReturn.notificationError->errorDetail;
	}
	else if (fatalReturn.transferBatchError) {
		errorType = "Tranfer Batch";
		pErrDetailList = &fatalReturn.transferBatchError->errorDetail;
	}
	
	string operSpecInfo;
	if (fatalReturn.operatorSpecList) {
		// concatenate operator specific info before loading
		for (int i = 0; i < fatalReturn.operatorSpecList->list.count; i++) {
			if (operSpecInfo.length() > 0)
				operSpecInfo += "\r\n";
			operSpecInfo += (char*)fatalReturn.operatorSpecList->list.array[i]->buf;
		}
		if (operSpecInfo.length() > 1024)
			operSpecInfo = operSpecInfo.substr(0, 1024);
	}

	otl_nocommit_stream otlStream;
	long returnID;
	otlStream.open(1 /*stream buffer size in logical rows*/,
		"insert into BILLING.RAP_FATAL_RETURN (RETURN_ID, FILE_ID, FILE_SEQUENCE_NUMBER, ERROR_TYPE, OPERATOR_SPEC_INFO) \
						values (Billing.TAP3EventID.NextVal, :hfileid /*long,in*/, :fileseqnum /*char[20],in*/, \
						:error_type /*char[50],in*/, :oper_spec_info /*char[1024],in*/) \
						returning RETURN_ID into :hreturnid /*long,out*/", otlConnect);

	otlStream
		<< fileID
		<< fatalReturn.fileSequenceNumber.buf
		<< errorType
		<< operSpecInfo;

	otlStream.flush();
	otlStream >> returnID;
	otlStream.close();

	return LoadRAPErrorDetailList(pErrDetailList, returnID);
}

//------------------------------------------------

int LoadRAPSevereReturn(long fileID, const SevereReturn& severeReturn)
{
	int index = 0;

	string operSpecInfo;
	if (severeReturn.operatorSpecList) {
		// concatenate operator specific info before loading
		for (int i = 0; i < severeReturn.operatorSpecList->list.count; i++) {
			if (operSpecInfo.length() > 0)
				operSpecInfo += "\r\n";
			operSpecInfo += (char*)severeReturn.operatorSpecList->list.array[i]->buf;
		}
		if (operSpecInfo.length() > 1024)
			operSpecInfo = operSpecInfo.substr(0, 1024);
	}
	
	long long eventID;
	switch (severeReturn.callEventDetail.present) {
		case CallEventDetail_PR_mobileOriginatedCall:
			if ((eventID = ProcessOriginatedCall(fileID, index, &severeReturn.callEventDetail.choice.mobileOriginatedCall)) < 0)
				return (long) eventID;
			
			break;
		case CallEventDetail_PR_mobileTerminatedCall:
			if ((eventID = ProcessTerminatedCall(fileID, index, &severeReturn.callEventDetail.choice.mobileTerminatedCall)) < 0)
				return (long) eventID;
			
			break;
		case CallEventDetail_PR_supplServiceEvent:
			// at this time we just ignore it
			break;

		case CallEventDetail_PR_gprsCall:
			if ((eventID = ProcessGPRSCall(fileID, index, &severeReturn.callEventDetail.choice.gprsCall)) < 0)
				return (long) eventID;
			
			break;
		default:
			if (!debugMode) {
				log(LOG_ERROR, string("No handler for call event detail type=") + to_string(static_cast<unsigned long long> (dataInterchange->choice.transferBatch.callEventDetails->list.array[index - 1]->present)) +
					string(". Call number=") + to_string(static_cast<unsigned long long> (index)));
				return TL_NEWCOMPONENT;
			}
	}

	otl_nocommit_stream otlStream;
	long returnID;
	otlStream.open(1 /*stream buffer size in logical rows*/,
		"insert into BILLING.RAP_SEVERE_RETURN (RETURN_ID, FILE_ID, FILE_SEQUENCE_NUMBER, EVENT_ID, OPERATOR_SPEC_INFO) \
				values (Billing.TAP3EventID.NextVal, :hfileid /*long,in*/, :fileseqnum /*char[20],in*/, :event_id /*bigint,in*/, :oper_spec_info /*char[1024],in*/) \
					returning RETURN_ID into :hreturnid /*long,out*/", otlConnect);

	otlStream
		<< fileID
		<< severeReturn.fileSequenceNumber.buf
		<< eventID
		<< operSpecInfo;

	otlStream.flush();
	otlStream >> returnID;
	otlStream.close();

	return LoadRAPErrorDetailList(&severeReturn.errorDetail, returnID);
}

//-----------------------------------------------------

int LoadReturnBatchToDB(ReturnBatch* returnBatch, long fileID, long roamingHubID, string rapFilename, long fileStatus)
			{
		// set TAP power value used to convert integer values from file to double values for DB
	double dblTAPPower;
		if( returnBatch->rapBatchControlInfoRap.tapDecimalPlaces )
		dblTAPPower = pow( (double) 10, *returnBatch->rapBatchControlInfoRap.tapDecimalPlaces );
		else
		dblTAPPower = 1;

		otl_nocommit_stream otlStream;
	try {	
		// REGISTER RAP FILE IN DB 
		otlStream.open( 1 /*stream buffer size in logical rows*/, 
		"insert into BILLING.RAP_FILE (FILE_ID, ROAMINGHUB_ID, FILENAME, SENDER, RECIPIENT, ROAMING_PARTNER, SEQUENCE_NUMBER , CREATION_STAMP, CREATION_UTCOFF,"
		"AVAILABLE_STAMP, AVAILABLE_UTCOFF, TAP_CURRENCY, LOAD_TIME, "
		"RETURN_DETAILS_COUNT, TOTAL_SEVERE_RETURN, TOTAL_SEVERE_RETURN_TAX, STATUS, RAP_VERSION, RAP_RELEASE, FILE_TYPE_INDICATOR, "
		"TAP_DECIMAL_PLACES, TAP_VERSION, TAP_RELEASE) "
		"values ("
		":hfileid /*long,in*/, :roamhubid /*long,in*/, :filename/*char[255],in*/, :sender/*char[20],in*/, :recipient/*char[20],in*/, :roam_partner/*char[10],in*/, :seq_num/*char[10],in*/,"
		"to_date(:creation_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :creation_utcoff /* char[10],in */,"
		"to_date(:available_stamp /*char[20],in*/, 'yyyymmddhh24miss'), :available_utcoff /* char[10],in */,"
		":tap_currency /* char[255],in */, sysdate, :eventcount /* long,in */, :total_ret /* double,in */, :total_ret_tax /*double,in*/, :status /*long,in*/, "
		":rapVer /*long,in*/, :rapSpecif /*long,in*/, :filetype /*char[5],in*/, :tapDecimalPlaces /*long,in*/, "
		":tapVersion /*long,in*/, :tapSpecif /*long,in*/ ) ", otlConnect);

		otlStream 
			<< fileID
			<< roamingHubID
			<< rapFilename 
			<< returnBatch->rapBatchControlInfoRap.sender.buf
			<< returnBatch->rapBatchControlInfoRap.recipient.buf
			<< (returnBatch->rapBatchControlInfoRap.roamingPartner ? (const char*) returnBatch->rapBatchControlInfoRap.roamingPartner->buf : "" )
			<< returnBatch->rapBatchControlInfoRap.rapFileSequenceNumber.buf
			<< (const char*) returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.localTimeStamp->buf
			<< (const char*) returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.utcTimeOffset->buf
			<< (const char*) returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.localTimeStamp->buf
			<< (const char*) returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.utcTimeOffset->buf
			<< ( returnBatch->rapBatchControlInfoRap.tapCurrency ? (const char*) returnBatch->rapBatchControlInfoRap.tapCurrency->buf : "" )
			<< returnBatch->rapAuditControlInfo.returnDetailsCount
			<< OctetStr2Int64(returnBatch->rapAuditControlInfo.totalSevereReturnValue) / dblTAPPower;

		if( returnBatch->rapAuditControlInfo.totalSevereReturnTax )
			otlStream << OctetStr2Int64(*returnBatch->rapAuditControlInfo.totalSevereReturnTax) / dblTAPPower;
		else
			otlStream << otl_null();

		otlStream
		<< fileStatus
			<< returnBatch->rapBatchControlInfoRap.rapSpecificationVersionNumber
			<< returnBatch->rapBatchControlInfoRap.rapReleaseVersionNumber
			<< (returnBatch->rapBatchControlInfoRap.fileTypeIndicator ? (const char*) returnBatch->rapBatchControlInfoRap.fileTypeIndicator->buf : "" );

		if( returnBatch->rapBatchControlInfoRap.tapDecimalPlaces  )
			otlStream << *returnBatch->rapBatchControlInfoRap.tapDecimalPlaces;
		else
			otlStream << otl_null();

		if( returnBatch->rapBatchControlInfoRap.specificationVersionNumber  )
			otlStream << *returnBatch->rapBatchControlInfoRap.specificationVersionNumber;
		else
			otlStream << otl_null();

		if( returnBatch->rapBatchControlInfoRap.releaseVersionNumber )
			otlStream << *returnBatch->rapBatchControlInfoRap.releaseVersionNumber;
		else
			otlStream << otl_null();

		otlStream.flush();
		otlStream.close();

		int loadResult = -1;
		for (int i = 0; i < returnBatch->returnDetails.list.count; i++) {
			switch (returnBatch->returnDetails.list.array[i]->present) {
			case ReturnDetail_PR_stopReturn:
				loadResult = LoadRAPStopOrMissingInfo(fileID, (char*)returnBatch->returnDetails.list.array[i]->choice.stopReturn.lastSeqNumber.buf, "", "",
					returnBatch->returnDetails.list.array[i]->choice.stopReturn.operatorSpecList);
				break;
			case ReturnDetail_PR_missingReturn:
				loadResult = LoadRAPStopOrMissingInfo(fileID, "",
					(char*) returnBatch->returnDetails.list.array[i]->choice.missingReturn.startMissingSeqNumber.buf, 
					returnBatch->returnDetails.list.array[i]->choice.missingReturn.endMissingSeqNumber ?
						(char*) returnBatch->returnDetails.list.array[i]->choice.missingReturn.endMissingSeqNumber->buf : "",
					returnBatch->returnDetails.list.array[i]->choice.stopReturn.operatorSpecList);
				break;
			case ReturnDetail_PR_fatalReturn:
				loadResult = LoadRAPFatalReturn(fileID, returnBatch->returnDetails.list.array[i]->choice.fatalReturn);
				break;
			case ReturnDetail_PR_severeReturn:
				loadResult = LoadRAPSevereReturn(fileID, returnBatch->returnDetails.list.array[i]->choice.severeReturn);
				break;
			default:
				log(LOG_ERROR, "Unknown return detail structure: " + to_string(static_cast<unsigned long long> (returnBatch->returnDetails.list.array[i]->present)));
				return TL_DECODEERROR;
			}

			if (loadResult != TL_OK)
				return loadResult;
		}
	}
	catch (otl_exception &otlEx) {
		otlConnect.rollback();
		log( LOG_ERROR, "DB error while processing CDR records:");
		log( LOG_ERROR, (char*) otlEx.msg);
		if( strlen(otlEx.stm_text) > 0 )
			log( LOG_ERROR, (char*) otlEx.stm_text ); // log SQL that caused the error
		if( strlen(otlEx.var_info) > 0 )
			log( LOG_ERROR, (char*) otlEx.var_info ); // log the variable that caused the error
		return TL_ORACLEERROR;
	}
	return TL_OK;
}

//--------------------------------------------------

int LoadRAPFileToDB( unsigned char* buffer, long dataLen, long fileID, long roamingHubID, bool bPrintOnly ) 
{
	int index=0;
	try {
		asn_dec_rval_t rval;

		rval = ber_decode(0, &asn_DEF_ReturnBatch, (void**) &returnBatch, buffer, dataLen);

		if (rval.code != RC_OK) {
			log(LOG_ERROR, string("Error while decoding ASN file. Error code ") + to_string(static_cast<unsigned long long> ( rval.code )));
			Finalize();
			return TL_DECODEERROR;
		}

		if (bPrintOnly) {
			char* printName = new char[strlen(pShortName) + 5];
			sprintf(printName, "%s.txt", pShortName);
			FILE* fFileContents = fopen(printName, "w");
			if (fFileContents) {
				asn_fprint(fFileContents, &asn_DEF_ReturnBatch, returnBatch);
				fclose(fFileContents);
			}
			else
			{
				printf("Unable to open output file %s\n", printName);
			}
			delete[] printName;
			printf("---- File contents printed to output file. Exiting. -------");
			return TL_OK;
		}
	
		return LoadReturnBatchToDB(returnBatch, fileID, roamingHubID, pShortName, INFILE_STATUS_NEW);
	}
	catch(char* pMess)
	{
		log(LOG_ERROR, string("Exception caught: ") + string(pMess) + string(". Call number=") + to_string(static_cast<unsigned long long> (index)));
		return TL_WRONGCODE;
	}
	return TL_OK;
}

//------------------------------

int LoadRAPAckToDB(unsigned char* buffer, long dataLen, long fileID, long roamingHubID, bool bPrintOnly)
{
	asn_dec_rval_t rval;

	rval = ber_decode(0, &asn_DEF_Acknowledgement, (void**)& acknowledgement, buffer, dataLen);

	if (rval.code != RC_OK) {
		log(LOG_ERROR, string("Error while decoding ASN file. Error code ") + to_string(static_cast<unsigned long long> (rval.code)));
		Finalize();
		return TL_DECODEERROR;
	}

	if (bPrintOnly) {
		char* printName = new char[strlen(pShortName) + 5];
		sprintf(printName, "%s.txt", pShortName);
		FILE* fFileContents = fopen(printName, "w");
		if (fFileContents) {
			asn_fprint(fFileContents, &asn_DEF_Acknowledgement, acknowledgement);
			fclose(fFileContents);
		}
		else
		{
			printf("Unable to open output file %s\n", printName);
		}
		delete[] printName;
		printf("---- File contents printed to output file. Exiting. -------");
		return TL_OK;
	}
	
	// We do not load RAP ack to DB, just log it and then update RAP file STATUS and ACK_RECEIVED fields
	log(LOG_INFO, string("Sender: ") + (char*)acknowledgement->sender.buf + ", Recipient: " + (char*)acknowledgement->recipient.buf + 
				", RAP file seqnum: " + (char*)acknowledgement->rapFileSequenceNumber.buf);
	log(LOG_INFO, string("Ack file creation: ") + (char*)acknowledgement->ackFileCreationTimeStamp.localTimeStamp->buf + 
					", Ack file creation UTC: " + (char*)acknowledgement->ackFileCreationTimeStamp.utcTimeOffset->buf +
					", Ack file available: " + (char*)acknowledgement->ackFileAvailableTimeStamp.localTimeStamp->buf +
					", Ack file available UTC: " + (char*)acknowledgement->ackFileAvailableTimeStamp.utcTimeOffset->buf
					);

	try {
		// UPDATE RAP file STATUS and ACK_RECEIVED fields
		otl_nocommit_stream otlStream;
		otlStream.open(1, "UPDATE BILLING.RAP_FILE set status=:hStatus /*long,in*/, ack_received=sysdate where sender=:sender /*char[20]*/ \
								and recipient=:recip/*char[20]*/ and sequence_number=:seqnum /*char[20]*/ ", otlConnect);
		otlStream
			<< (long) OUTRAP_ACKNOWLEDGE_RECEIVED	
			<< (const char*) acknowledgement->sender.buf
			<< (const char*)acknowledgement->recipient.buf
			<< (const char*)acknowledgement->rapFileSequenceNumber.buf
			;

		otlStream.flush();
		otlStream.close();
	}
	catch (otl_exception &otlEx) {
		otlConnect.rollback();
		log(LOG_ERROR, "DB error while processing acknowledgement file:");
		log(LOG_ERROR, (char*)otlEx.msg);
		if (strlen(otlEx.stm_text) > 0)
			log(LOG_ERROR, (char*)otlEx.stm_text); // log SQL that caused the error
		if (strlen(otlEx.var_info) > 0)
			log(LOG_ERROR, (char*)otlEx.var_info); // log the variable that caused the error
		return TL_ORACLEERROR;
	}

	return TL_OK;
}

//------------------------------

int main(int argc, const char* argv[])
{
	if( argc < mainArgsCount )
		return TL_PARAM_ERROR;

	// установим pShortName на имя файла, отбросив путь
	pShortName =  strrchr(argv[1],'\\');
	if(!pShortName)
		pShortName=argv[1];
	else
		pShortName++;

	// откроем файл для логгирования
	ofsLog.open("TAP3Loader.log", ofstream::app);
	if (!ofsLog.is_open())
		fprintf(stderr, "Unable to open log file TAP3Loader.log");

	long fileID = strtol(argv[2], NULL, 10);
	if( fileID == 0 ) {
		log( LOG_ERROR, string("Wrong value of fileID given in 2nd parameter: ") + argv[2] );
		return TL_FILEERROR ;
	}

	long roamingHubID = strtol(argv[3], NULL, 10);
	if( roamingHubID == 0 ) {
		log( LOG_ERROR, string("Wrong value of roamingHubID given in 3nd parameter: ") + argv[3] );
		return TL_FILEERROR ;
	}

	// установим тип файла
	FileType fileType;
	if( !strnicmp(pShortName, "CD", 2) || !strnicmp(pShortName, "TD", 2))
		fileType = ftTAP;
	else if( !strnicmp(pShortName, "RC", 2) || !strnicmp(pShortName, "RT", 2))
		fileType = ftRAP;
	else if( !strnicmp(pShortName, "AC", 2) || !strnicmp(pShortName, "AT", 2) )
		fileType = ftRAPAcknowledgement;
	else {
		log(LOG_ERROR, string("Unknown type of TAP file. First letters of filename must be CD, TD, RC, RT, AC or AT. Given filename is ") + argv[1]);
		Finalize();
		return TL_FILEERROR;
	}

	int index=0;
	try {
		// чтение файла конфигурации
		const char* configFilename = strlen(argv[4]) > 0 ? argv[4] : "TAP3Loader.cfg";
		ifstream ifsSettings(configFilename, ifstream::in);
		if (!ifsSettings.is_open())	{
			log( LOG_ERROR, string("Unable to open config file ") + configFilename);
			if( buffer ) delete [] buffer;
			return TL_PARAM_ERROR;
		}
		config.ReadConfigFile(ifsSettings);
		ifsSettings.close();

		if (config.GetConnectString().empty()) {
			log(LOG_ERROR, string("Connect string to DB is not found in ") + configFilename + ". Exiting.");
			ofsLog.close();
			exit(TL_FILEERROR);
		}

		FILE *fTapFile=fopen(argv[1],"rb");
		if(!fTapFile) {
			log( LOG_ERROR, string ("Unable to open input file ") + argv[1]);
			return TL_PARAM_ERROR;
		}

		fseek(fTapFile, 0, SEEK_END);
		unsigned long tapFileLen=ftell(fTapFile); // длина данных файла (без заголовка)
	
		buffer = new unsigned char [tapFileLen];

		fseek(fTapFile, 0, SEEK_SET);
		size_t bytesRead=fread(buffer, 1, tapFileLen, fTapFile);
		fclose(fTapFile);
		if(bytesRead < tapFileLen)
		{
			log( LOG_ERROR, string("Error reading file ") + argv[1]);
			delete [] buffer;
			return TL_FILEERROR;
		}

		bool bPrintOnly = false;
		if(argc > mainArgsCount) {
			if(!strcmp(argv[mainArgsCount], "-p") || !strcmp(argv[mainArgsCount], "-P")) {
				// key to print contents of file. No upload to DB is needed.
				bPrintOnly = true;
			}

			if(!strcmp(argv[mainArgsCount], "-d") || !strcmp(argv[mainArgsCount], "-D")) {
				// debugMode = 1;
			}
		}

		// DB connect
		otl_connect::otl_initialize(); // initialize OCI environment
		try {
			otlConnect.rlogon(config.GetConnectString().c_str());	
			otlLogConnect.rlogon(config.GetConnectString().c_str());	
		}
		catch (otl_exception &otlEx) {
			log( LOG_ERROR, "Unable to connect to DB:" );
			log( LOG_ERROR, (char*) otlEx.msg );
			if( strlen(otlEx.stm_text) > 0 )
				log( LOG_ERROR, (char*) otlEx.stm_text ); // log SQL that caused the error
			if( strlen(otlEx.var_info) > 0 )
				log( LOG_ERROR, (char*) otlEx.var_info ); // log the variable that caused the error
			log( LOG_ERROR, "---- TAP3 loader finished with errors, see DB log----");
			if(ofsLog.is_open()) ofsLog.close();
			return TL_CONNECTERROR; 
		}
		
		int res;
		switch( fileType ) {
		case ftTAP:
			log(LOG_INFO, "--------- Loading TAP3 file (ID " + to_string((long long) fileID)+") started ---------");
			res = LoadTAPFileToDB( buffer, tapFileLen, fileID, roamingHubID, bPrintOnly ) ; 
			if (res == TL_OK)
				log(LOG_INFO, "--------- Loading TAP3 file (ID " + to_string((long long)fileID) + ") finished successfully ---------");
			else
				log(LOG_INFO, "--------- Loading TAP3 file file (ID " + to_string((long long)fileID) + ") finished with errors ---------");
			Finalize( res == TL_OK );
			return res;
		case ftRAP:
			log(LOG_INFO, "--------- Loading RAP file (ID " + to_string((long long) fileID) + ") started ---------");
			res = LoadRAPFileToDB( buffer, tapFileLen, fileID, roamingHubID, bPrintOnly ) ; 
			if (res == TL_OK)
				log(LOG_INFO, "--------- Loading RAP file (ID " + to_string((long long)fileID) + ") finished successfully ---------");
			else
				log(LOG_INFO, "--------- Loading RAP file file (ID " + to_string((long long)fileID) + ") finished with errors ---------");
			Finalize( res == TL_OK );
			return res;
		case ftRAPAcknowledgement:
			log(LOG_INFO, "--------- Loading RAP Acknowledgement file (ID " + to_string((long long) fileID) + ") started ---------");
			res = LoadRAPAckToDB(buffer, tapFileLen, fileID, roamingHubID, bPrintOnly);
			if (res == TL_OK)
				log(LOG_INFO, "--------- Loading RAP Acknowledgement file (ID " + to_string((long long)fileID) + ") finished successfully ---------");
			else
				log(LOG_INFO, "--------- Loading RAP Acknowledgement file (ID " + to_string((long long)fileID) + ") finished with errors ---------");
			Finalize( res == TL_OK );
			return res;
		}
		
	}
	catch(...)
	{
		log( LOG_ERROR, "Unknown exception caught. Call number=" + to_string( static_cast<unsigned long long> (index)));
		Finalize();
		return TL_UNKNOWN;
	}

	Finalize(true);

	return TL_OK;
}

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
	switch (fdwReason)
	{
	case DLL_PROCESS_ATTACH:
		break;
	case DLL_THREAD_ATTACH:
		// A process is creating a new thread.
		break;
	case DLL_THREAD_DETACH:
		// A thread exits normally.
		break;
	case DLL_PROCESS_DETACH:
		break;
	}
	return TRUE;
}

//---------------------------------
// Function exported by DLL
//---------------------------------
__declspec (dllexport) int __stdcall LoadFileToDB(char* pFilename, long fileID, long roamingHubID, char* pConfigFilename)
{
	/*ofstream params("params.txt");
	if(params.is_open()) {
		params << "filename: " << pFilename << endl;
		params << "fileID: " << fileID << endl;
		params << "roamingHubID: " << roamingHubID << endl;
		params << "pConfigFilename: " << pConfigFilename << endl;
	}
	params.close();*/

	string strFileID = to_string ((unsigned long long) fileID);
	string strRoamHubID = to_string((unsigned long long) roamingHubID);
	const char* pArgv[] = { "TAP3Loader.exe", pFilename, strFileID.c_str(), strRoamHubID.c_str(), pConfigFilename };

	return main(mainArgsCount, pArgv);
}