// Class TAPValidator checks TAP file (DataInterchange structure) validity according to TD.57 requirements.
// If Fatal or Severe errors are found it creates RAP file (Return Batch structure) and registers it in DB tables (RAP_File, RAP_Fatal_Return and so on).

#include <math.h>
#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "TAPValidator.h"

using namespace std;

extern void log(string filename, short msgType, string msgText);
extern void log(short msgType, string msgText);
extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);

TAPValidator::TAPValidator(otl_connect& dbConnect, Config& config, string rapFilename, string roamingHubName) 
	: m_otlConnect(dbConnect), m_config(config), m_rapFilename(rapFilename), m_roamingHubName(roamingHubName)
{
}


bool TAPValidator::UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting)
{
	try {
		if (ftpSetting.ftpPort.length() == 0)
			ftpSetting.ftpPort = "21";	// use default ftp port
		int ncftp_argc = 11;
		const char* pszArguments[] = { "ncftpput", "-u", ftpSetting.ftpUsername.c_str(), "-p", ftpSetting.ftpPassword.c_str(), "-P", ftpSetting.ftpPort.c_str(),
			ftpSetting.ftpServer.c_str(), ftpSetting.ftpDirectory.c_str(), fullFileName.c_str(), NULL };
		char szFtpResult[4096];
		int ftpResult = ncftp_main(ncftp_argc, (char**) pszArguments, szFtpResult);
		if (ftpResult != 0) {
			log(filename, LOG_ERROR, "Error while uploading file " + filename + " on FTP server " + ftpSetting.ftpServer + ":");
			log(filename, LOG_ERROR, szFtpResult);
			return false;
		}
		log(filename, LOG_INFO, "Successful upload to FTP server " + ftpSetting.ftpServer + " for " + m_roamingHubName);
		return true;
	}
	catch (...) {
		log(filename, LOG_ERROR, "Exception while uploading " + filename + " to FTP server " + ftpSetting.ftpServer + ". Uploading failed.");
		return false;
	}
}


int TAPValidator::EncodeAndUpload(string filename, string fileTypeDescr, asn_TYPE_descriptor_t* pASNTypeDescr, ReturnBatch* pASNStructure)
{
	string fullFileName;
#ifdef WIN32
	fullFileName = (m_config.GetOutputDirectory().empty() ? "." : m_config.GetOutputDirectory()) + "\\" + filename;
#else
	fullFileName = (m_config.GetOutputDirectory().empty() ? "." : m_config.GetOutputDirectory()) + "/" + filename;
#endif

	FILE *fTapFile = fopen(fullFileName.c_str(), "wb");
	if (!fTapFile) {
		log(filename, LOG_ERROR, string("Unable to open file ") + fullFileName + " for writing.");
		return TL_FILEERROR;
	}
	asn_enc_rval_t encodeRes = der_encode(pASNTypeDescr, pASNStructure, write_out, fTapFile);

	fclose(fTapFile);

	if (encodeRes.encoded == -1) {
		log(filename, LOG_ERROR, string("Error while encoding ASN file. Error code ") + string(encodeRes.failed_type ? encodeRes.failed_type->name : "unknown"));
		return TL_DECODEERROR;
	}

	log(filename, LOG_INFO, fileTypeDescr + " successfully created.");

	// Upload file to FTP-server
	FtpSetting ftpSetting = m_config.GetFTPSetting(m_roamingHubName);
	if (!ftpSetting.ftpServer.empty()) {
		if (!UploadFileToFtp(filename, fullFileName, ftpSetting)) {
			return TL_FILEERROR;
		}
	}
	else
		log(filename, LOG_INFO, "FTP server is not set in cfg-file for roaming hub " + m_roamingHubName + ". No uploading done.");

	return TL_OK;
}


int TAPValidator::OctetString_fromInt64(OCTET_STRING& octetStr, long long value)
{
	unsigned char buf[8];
	int i;
	// fill buffer with value, most significant bytes first, less significant - last
	for (i = 7; i >= 0; i--) {
		buf[i] = value & 0xFF;
		value >>= 8;
		if (value == 0) break;
	}
	if (i == 0 && value > 0)
		throw "8-byte integer overflow";

	if (buf[i] >= 0x80) {
		// it will be treated as negative value, so add one more byte
		if (i == 0)
			throw "8-byte integer overflow";
		buf[--i] = 0;
	}

	OCTET_STRING_fromBuf(&octetStr, (const char*) ( buf + i ), 8 - i);

	return 8 - i;
}

bool TAPValidator::BatchContainsTaxes()
{
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
					return true;
		}
	}
	return false;
}

bool TAPValidator::BatchContainsDiscounts()
{
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
					return true;
		}
	}
	return false;
}

bool TAPValidator::ChargeInfoContainsPositiveCharges(ChargeInformation* chargeInfo)
{
	double tapPower = pow( (double) 10, *m_transferBatch->accountingInfo->tapDecimalPlaces);
	for (int chr_det_index = 0; chr_det_index < chargeInfo->chargeDetailList->list.count; chr_det_index++) {
		double charge = OctetStr2Int64(*chargeInfo->chargeDetailList->list.array[chr_det_index]->charge) / tapPower;
		if (charge > 0)
			return true;
	}
	return false;
}

bool TAPValidator::BatchContainsPositiveCharges()
{
	
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++)
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (ChargeInfoContainsPositiveCharges(
							moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]))
						return true;
		}
		
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++) 
					if (ChargeInfoContainsPositiveCharges(
							mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]))
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (ChargeInfoContainsPositiveCharges(
						gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]))
					return true;
		}
	}
	return false;
}

ReturnBatch* TAPValidator::FillReturnBatch(ReturnDetail* returnDetail)
{
// TODO: get this params from DB
		string rapSequenceNumber = "00001"; 
		string rapCreationStamp = "20151014100000";
		string ourUTCOffset = "+0300";
		long tapVersion = 3;
		long tapRelease = 12;
		long rapVersion = 1;
		long rapRelease = 5;
		long tapDecimalPlaces = 6;
// END TODO
	
	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));

	// sender and recipient switch their places
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.sender, (const char*) m_transferBatch->batchControlInfo->sender->buf,
		m_transferBatch->batchControlInfo->recipient->size);
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.recipient, (const char*) m_transferBatch->batchControlInfo->recipient->buf,
		m_transferBatch->batchControlInfo->recipient->size);

	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.rapFileSequenceNumber, rapSequenceNumber.c_str(), rapSequenceNumber.size());
	returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.localTimeStamp =
		OCTET_STRING_new_fromBuf (&asn_DEF_LocalTimeStamp, rapCreationStamp.c_str(), rapCreationStamp.size());
	returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.utcTimeOffset =
		OCTET_STRING_new_fromBuf (&asn_DEF_UtcTimeOffset, ourUTCOffset.c_str(), ourUTCOffset.size());
	returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.localTimeStamp =
		OCTET_STRING_new_fromBuf (&asn_DEF_LocalTimeStamp, rapCreationStamp.c_str(), rapCreationStamp.size());
	returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.utcTimeOffset =
		OCTET_STRING_new_fromBuf (&asn_DEF_UtcTimeOffset, ourUTCOffset.c_str(), ourUTCOffset.size());

	if (tapDecimalPlaces != -1) {
		returnBatch->rapBatchControlInfoRap.tapDecimalPlaces = (TapDecimalPlaces_t*) calloc(1, sizeof(TapDecimalPlaces_t));
		*returnBatch->rapBatchControlInfoRap.tapDecimalPlaces = tapDecimalPlaces;
	}

	returnBatch->rapBatchControlInfoRap.rapSpecificationVersionNumber = rapVersion;
	returnBatch->rapBatchControlInfoRap.rapReleaseVersionNumber = rapRelease;
	returnBatch->rapBatchControlInfoRap.specificationVersionNumber = (SpecificationVersionNumber_t*) calloc(1, sizeof(SpecificationVersionNumber_t));
	*returnBatch->rapBatchControlInfoRap.specificationVersionNumber = tapVersion;
	returnBatch->rapBatchControlInfoRap.releaseVersionNumber = (ReleaseVersionNumber_t*) calloc(1, sizeof(ReleaseVersionNumber_t));
	*returnBatch->rapBatchControlInfoRap.releaseVersionNumber = tapRelease;
			
	if (m_transferBatch->batchControlInfo->fileTypeIndicator)
		returnBatch->rapBatchControlInfoRap.fileTypeIndicator = OCTET_STRING_new_fromBuf(&asn_DEF_FileTypeIndicator, 
			(const char*)m_transferBatch->batchControlInfo->fileTypeIndicator->buf, 
			m_transferBatch->batchControlInfo->fileTypeIndicator->size);
		
// TODO: Operator specific info is mandatory for IOT errors (TD.52 RAP implementation handbook)
// Fill it for Severe errors
			
	ASN_SEQUENCE_ADD(&returnBatch->returnDetails, returnDetail);

	OctetString_fromInt64(returnBatch->rapAuditControlInfo.totalSevereReturnValue, (long long) 0);
	returnBatch->rapAuditControlInfo.returnDetailsCount = 1; // For Fatal errors 

	return returnBatch;
	
}


int TAPValidator::CreateTransferBatchRAPFile(string logMessage, int errorCode)
{
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*)m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.transferBatchError = (TransferBatchError*) calloc(1, sizeof(TransferBatchError));
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it
	errorContext1level->itemLevel = 1;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.transferBatchError->errorDetail, errorDetail);
	
	ReturnBatch* returnBatch = FillReturnBatch(returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	return encodeAndUploadRes;
}

int TAPValidator::CreateBatchControlInfoRAPFile(string logMessage, int errorCode)
{
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, 
		(const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.batchControlError = (BatchControlError*) calloc(1, sizeof(BatchControlError));
	
	//Copy batchControlInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.sender =
		m_transferBatch->batchControlInfo->sender;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.recipient = 
		m_transferBatch->batchControlInfo->recipient;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileAvailableTimeStamp = 
		m_transferBatch->batchControlInfo->fileAvailableTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileCreationTimeStamp = 
		m_transferBatch->batchControlInfo->fileCreationTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.transferCutOffTimeStamp = 
		m_transferBatch->batchControlInfo->transferCutOffTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileSequenceNumber = 
		m_transferBatch->batchControlInfo->fileSequenceNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileTypeIndicator = 
		m_transferBatch->batchControlInfo->fileTypeIndicator;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.operatorSpecInformation = 
		m_transferBatch->batchControlInfo->operatorSpecInformation;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.rapFileSequenceNumber = 
		m_transferBatch->batchControlInfo->rapFileSequenceNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.releaseVersionNumber = 
		m_transferBatch->batchControlInfo->releaseVersionNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.specificationVersionNumber = 
		m_transferBatch->batchControlInfo->specificationVersionNumber;
	
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = 1;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_BatchControlInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = 2;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.batchControlError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = FillReturnBatch(returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.sender = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.recipient = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileAvailableTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileCreationTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.transferCutOffTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileSequenceNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileTypeIndicator = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.operatorSpecInformation = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.rapFileSequenceNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.releaseVersionNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.specificationVersionNumber = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);

	return encodeAndUploadRes;
}


int TAPValidator::CreateAccountingInfoRAPFile(string logMessage, int errorCode)
{
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.accountingInfoError = (AccountingInfoError*) calloc(1, sizeof(AccountingInfoError));
	
	//Copy AccountingInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.currencyConversionInfo = 
		m_transferBatch->accountingInfo->currencyConversionInfo;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.discounting = 
		m_transferBatch->accountingInfo->discounting;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.localCurrency = 
		m_transferBatch->accountingInfo->localCurrency;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapCurrency = 
		m_transferBatch->accountingInfo->tapCurrency;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = 
		m_transferBatch->accountingInfo->tapDecimalPlaces;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.taxation = 
		m_transferBatch->accountingInfo->taxation;
		
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = 1;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_AccountingInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = 2;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.accountingInfoError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = FillReturnBatch(returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.currencyConversionInfo = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.discounting = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.localCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.taxation = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);

	return encodeAndUploadRes;
}


int TAPValidator::CreateNetworkInfoRAPFile(string logMessage, int errorCode)
{
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.networkInfoError = (NetworkInfoError*) calloc(1, sizeof(NetworkInfoError));
	
	//Copy NetworkInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.recEntityInfo = 
		m_transferBatch->networkInfo->recEntityInfo;
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.utcTimeOffsetInfo = 
		m_transferBatch->networkInfo->utcTimeOffsetInfo;
		
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = 1;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_NetworkInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = 2;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.networkInfoError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = FillReturnBatch(returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.recEntityInfo = NULL;
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.utcTimeOffsetInfo = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);

	return encodeAndUploadRes;
}


TAPValidationResult TAPValidator::ValidateTransferBatch()
{
	// check mandatory structures in Transfer Batch
	if (!m_transferBatch->batchControlInfo) {
		CreateTransferBatchRAPFile("Batch Control Info missing in Transfer Batch", TF_BATCH_BATCH_CONTROL_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->accountingInfo) {
		CreateTransferBatchRAPFile("Accounting Info missing in Transfer Batch", TF_BATCH_ACCOUNTING_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->networkInfo) {
		CreateTransferBatchRAPFile("Network Info missing in Transfer Batch", TF_BATCH_NETWORK_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->auditControlInfo) {
		CreateTransferBatchRAPFile("Audit Control Info missing in Transfer Batch", TF_BATCH_AUDIT_CONTROL_INFO_MISSING);
		return FATAL_ERROR;
	}
		
	// check mandatory structures in Transfer Batch/Batch Control Information
	if (!m_transferBatch->batchControlInfo->sender || !m_transferBatch->batchControlInfo->recipient || !m_transferBatch->batchControlInfo->fileSequenceNumber) {
		log(LOG_ERROR, "Validation: Sender, Recipient or FileSequenceNumber is missing in Batch Control Info. Unable to create RAP file.");
		return VALIDATION_IMPOSSIBLE;
	}

	if (!m_transferBatch->batchControlInfo->fileAvailableTimeStamp) {
		CreateBatchControlInfoRAPFile("fileAvailableTimeStamp is missing in Batch Control Info", BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->batchControlInfo->specificationVersionNumber) {
		CreateBatchControlInfoRAPFile("specificationVersionNumber is missing in Batch Control Info", BATCH_CTRL_SPEC_VERSION_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->batchControlInfo->transferCutOffTimeStamp) {
		CreateBatchControlInfoRAPFile("transferCutOffTimeStamp is missing in Batch Control Info", BATCH_CTRL_TRANSFER_CUTOFF_MISSING);
		return FATAL_ERROR;
	}

	// check mandatory structures in Transfer Batch/Accounting Information
	if (!m_transferBatch->accountingInfo->localCurrency) {
		CreateAccountingInfoRAPFile("localCurrency is missing in Accounting Info", ACCOUNTING_LOCAL_CURRENCY_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->accountingInfo->tapDecimalPlaces) {
		CreateAccountingInfoRAPFile("tapDecimalPlaces is missing in Accounting Info", ACCOUNTING_TAP_DECIMAL_PLACES_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->accountingInfo->taxation && BatchContainsTaxes()) {
		CreateAccountingInfoRAPFile("taxation group is missing in Accounting Info and batch contains taxes", 
			ACCOUNTING_TAXATION_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->accountingInfo->discounting && BatchContainsDiscounts()) {
		CreateAccountingInfoRAPFile("discounting group is missing in Accounting Info and batch contains discounts", 
			ACCOUNTING_DISCOUNTING_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->accountingInfo->currencyConversionInfo && BatchContainsPositiveCharges()) {
		CreateAccountingInfoRAPFile("currencyConversion group is missing in Accounting Info and batch contains charges greater than 0", 
			ACCOUNTING_CURRENCY_CONVERSION_MISSING);
		return FATAL_ERROR;
	}

	// check mandatory structures in Transfer Batch/Networl Information
	if (!m_transferBatch->networkInfo->utcTimeOffsetInfo) {
		CreateNetworkInfoRAPFile("utcTimeOffsetInfo is missing in Network Info", NETWORK_UTC_TIMEOFFSET_MISSING);
		return FATAL_ERROR;
	}

	if (!m_transferBatch->networkInfo->recEntityInfo) {
		CreateNetworkInfoRAPFile("recEntityInfo is missing in Network Info", NETWORK_REC_ENTITY_MISSING);
		return FATAL_ERROR;
	}

	return TAP_VALID;
}


TAPValidationResult TAPValidator::ValidateNotification()
{
	return TAP_VALID;
}


TAPValidationResult TAPValidator::Validate(DataInterChange* dataInterchange)
{
	// TODO: add check
	// if file is not addressed for our network, then return WRONG_ADDRESSEE;
	switch (dataInterchange->present) {
		case DataInterChange_PR_transferBatch:
			m_transferBatch = &dataInterchange->choice.transferBatch;
			return ValidateTransferBatch();
		case DataInterChange_PR_notification:
			return ValidateNotification();
		default:
			return VALIDATION_IMPOSSIBLE;
	}
}