// Class TAPValidator checks TAP file (DataInterchange structure) validity according to TD.57 requirements.
// If Fatal or Severe errors are found it creates RAP file (Return Batch structure) and registers it in DB tables (RAP_File, RAP_Fatal_Return and so on).

#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "TAPValidator.h"

using namespace std;

extern void log(string filename, short msgType, string msgText);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);

TAPValidator::TAPValidator(otl_connect& dbConnect, Config& config) 
	: m_otlConnect(dbConnect), m_config(config)
{
}


bool TAPValidator::UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting, string roamingHubName)
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
		log(filename, LOG_INFO, "Successful upload to FTP server " + ftpSetting.ftpServer + " for " + roamingHubName);
		return true;
	}
	catch (...) {
		log(filename, LOG_ERROR, "Exception while uploading " + filename + " to FTP server " + ftpSetting.ftpServer + ". Uploading failed.");
		return false;
	}
}


int TAPValidator::EncodeAndUpload(string filename, string fileTypeDescr, asn_TYPE_descriptor_t* pASNTypeDescr, ReturnBatch* pASNStructure, string roamingHubName)
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
	FtpSetting ftpSetting = m_config.GetFTPSetting(roamingHubName);
	if (!ftpSetting.ftpServer.empty()) {
		if (!UploadFileToFtp(filename, fullFileName, ftpSetting, roamingHubName)) {
			return TL_FILEERROR;
		}
	}
	else
		log(filename, LOG_INFO, "FTP server is not set in cfg-file for roaming hub " + roamingHubName + ". No uploading done.");

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


ReturnBatch* TAPValidator::FillReturnBatch(const TransferBatch& transferBatch, ReturnDetail* returnDetail)
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
		string rapFilename = "RCRUS27NNNNN00001";
		string roamingHubName = "COMFONE_TEST";
// END TODO
	
	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));

	// sender and recipient switch their places
	memcpy(&returnBatch->rapBatchControlInfoRap.sender, transferBatch.batchControlInfo->sender,
		transferBatch.batchControlInfo->recipient->size);
	memcpy(&returnBatch->rapBatchControlInfoRap.recipient, transferBatch.batchControlInfo->recipient,
		transferBatch.batchControlInfo->sender->size);

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
			
	if (transferBatch.batchControlInfo->fileTypeIndicator)
		memcpy(&returnBatch->rapBatchControlInfoRap.fileTypeIndicator, transferBatch.batchControlInfo->fileTypeIndicator, 
			transferBatch.batchControlInfo->fileTypeIndicator->size);
		
// TODO: Operator specific info is mandatory for IOT errors (TD.52 RAP implementation handbook)
// Fill it for Severe errors
			
	ASN_SEQUENCE_ADD(&returnBatch->returnDetails, returnDetail);

	OctetString_fromInt64(returnBatch->rapAuditControlInfo.totalSevereReturnValue, (long long) 0);
	returnBatch->rapAuditControlInfo.returnDetailsCount = 1; // For Fatal errors 

	return returnBatch;
	
}


TAPValidationResult TAPValidator::ValidateTransferBatch(const TransferBatch& transferBatch)
{
	// check mandatory structures in Transfer Batch
	if (!transferBatch.batchControlInfo || !transferBatch.accountingInfo ||	!transferBatch.networkInfo ||
		!transferBatch.auditControlInfo || !transferBatch.callEventDetails)	{
		//log( LOG_ERROR, "Some mandatory structures are missing in Transfer Batch");
		//log( LOG_ERROR, "Creating RAP file");

		//CreateFatalRAPFile();

// TODO: get this params from DB
		string rapSequenceNumber = "00001"; 
		string rapCreationStamp = "20151014100000";
		string ourUTCOffset = "+0300";
		long tapVersion = 3;
		long tapRelease = 12;
		long rapVersion = 1;
		long rapRelease = 5;
		long tapDecimalPlaces = 6;
		string rapFilename = "RCRUS27NNNNN00001";
		string roamingHubName = "COMFONE_TEST";
// END TODO

		if (!transferBatch.auditControlInfo) {
			ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
			returnDetail->present = ReturnDetail_PR_fatalReturn;
			memcpy(&returnDetail->choice.fatalReturn.fileSequenceNumber, transferBatch.batchControlInfo->fileSequenceNumber, 
				transferBatch.batchControlInfo->fileSequenceNumber->size);
			returnDetail->choice.fatalReturn.transferBatchError = (TransferBatchError*) calloc(1, sizeof(TransferBatchError));
			ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
			errorDetail->errorCode = TF_BATCH_AUDIT_CONTROL_INFO_MISSING;
			ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.transferBatchError->errorDetail, errorDetail);

			ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);

			int encodeAndUploadRes = EncodeAndUpload(rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch, roamingHubName);
			ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
		}
		return FATAL_ERROR;
	}
	return TAP_VALID;
}


TAPValidationResult TAPValidator::ValidateNotification(const Notification& notification)
{
	return TAP_VALID;
}


TAPValidationResult TAPValidator::Validate(DataInterChange* dataInterchange)
{
	// TODO: add check
	// if file is not addressed for our network, then return WRONG_ADDRESSEE;
		
	switch (dataInterchange->present) {
		case DataInterChange_PR_transferBatch:
			return ValidateTransferBatch(dataInterchange->choice.transferBatch);
		case DataInterChange_PR_notification:
			return ValidateNotification(dataInterchange->choice.notification);
		default:
			return VALIDATION_IMPOSSIBLE;
	}
}