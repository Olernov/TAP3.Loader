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
extern void log(short msgType, string msgText);
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

int TAPValidator::GetASNStructSize(asn_TYPE_descriptor_t *td, void *sptr)
{
	if(!td || !sptr)
		return 0;

	int size = 0;
	for (int edx = 0; edx < td->elements_count; edx++) {
		asn_TYPE_member_t *elm = &td->elements[edx];
		void *memb_ptr;
		if (elm->flags & ATF_POINTER) {
			memb_ptr = *(void **) ( (char *) sptr + elm->memb_offset );
			ASN__PRIMITIVE_TYPE_t* member_type = (ASN__PRIMITIVE_TYPE_t*) ( (char *) sptr + elm->memb_offset );
			if (memb_ptr)
				//ASN_STRUCT_FREE(*elm->type, memb_ptr);
				size += _msize(memb_ptr);
				//size += GetASNStructSize(elm->type, memb_ptr);
		}
		else {
			memb_ptr = (void *) ( (char *) sptr + elm->memb_offset );
			ASN__PRIMITIVE_TYPE_t* member_type = (ASN__PRIMITIVE_TYPE_t*) ( (char *) sptr + elm->memb_offset );
			//ASN_STRUCT_FREE_CONTENTS_ONLY(*elm->type, memb_ptr);
			//size += GetASNStructSize(elm->type, memb_ptr);
			size += member_type->size;
		}
	}
	return size;
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
// END TODO
	
	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));

	// sender and recipient switch their places
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.sender, (const char*) transferBatch.batchControlInfo->sender->buf,
		transferBatch.batchControlInfo->recipient->size);
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.recipient, (const char*) transferBatch.batchControlInfo->recipient->buf,
		transferBatch.batchControlInfo->recipient->size);

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
		returnBatch->rapBatchControlInfoRap.fileTypeIndicator = OCTET_STRING_new_fromBuf(&asn_DEF_FileTypeIndicator, 
			(const char*)transferBatch.batchControlInfo->fileTypeIndicator->buf, 
			transferBatch.batchControlInfo->fileTypeIndicator->size);
		
// TODO: Operator specific info is mandatory for IOT errors (TD.52 RAP implementation handbook)
// Fill it for Severe errors
			
	ASN_SEQUENCE_ADD(&returnBatch->returnDetails, returnDetail);

	OctetString_fromInt64(returnBatch->rapAuditControlInfo.totalSevereReturnValue, (long long) 0);
	returnBatch->rapAuditControlInfo.returnDetailsCount = 1; // For Fatal errors 

	return returnBatch;
	
}


//ReturnDetail* TAPValidator::CreateTransferBatchError(const TransferBatch& transferBatch, int errorCode)
//{
//	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
//	returnDetail->present = ReturnDetail_PR_fatalReturn;
//	memcpy(&returnDetail->choice.fatalReturn.fileSequenceNumber, transferBatch.batchControlInfo->fileSequenceNumber,
//		transferBatch.batchControlInfo->fileSequenceNumber->size);
//	returnDetail->choice.fatalReturn.transferBatchError = (TransferBatchError*) calloc(1, sizeof(TransferBatchError));
//	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
//	errorDetail->errorCode = errorCode;
//
//	// Fill Error Context List
//	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
//	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
//	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it
//	errorContext1level->itemLevel = 1;
//	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);
//	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.transferBatchError->errorDetail, errorDetail);
//
//	return returnDetail;
//}


ReturnDetail* TAPValidator::CreateBatchControlInfoError(const TransferBatch& transferBatch, int errorCode)
{
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	/*memcpy(&returnDetail->choice.fatalReturn.fileSequenceNumber, transferBatch.batchControlInfo->fileSequenceNumber,
		GetASNStructSize(&asn_DEF_BatchControlInfo, transferBatch.batchControlInfo));*/
	returnDetail->choice.fatalReturn.batchControlError = (BatchControlError*) calloc(1, sizeof(BatchControlError));
	long s = GetASNStructSize(&asn_DEF_BatchControlInfo, transferBatch.batchControlInfo);
	s = GetASNStructSize(&asn_DEF_TransferBatch, (void*) &transferBatch);
	
	//Copy batchControlInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.sender = transferBatch.batchControlInfo->sender;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.recipient = transferBatch.batchControlInfo->recipient;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileAvailableTimeStamp = transferBatch.batchControlInfo->fileAvailableTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileCreationTimeStamp = transferBatch.batchControlInfo->fileCreationTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.transferCutOffTimeStamp = transferBatch.batchControlInfo->transferCutOffTimeStamp;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileSequenceNumber = transferBatch.batchControlInfo->fileSequenceNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileTypeIndicator = transferBatch.batchControlInfo->fileTypeIndicator;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.operatorSpecInformation = transferBatch.batchControlInfo->operatorSpecInformation;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.rapFileSequenceNumber = transferBatch.batchControlInfo->rapFileSequenceNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.releaseVersionNumber = transferBatch.batchControlInfo->releaseVersionNumber;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.specificationVersionNumber = transferBatch.batchControlInfo->specificationVersionNumber;
	
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

	return returnDetail;
}

int TAPValidator::CreateTransferBatchRAPFile(string logMessage, const TransferBatch& transferBatch, int errorCode)
{
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	//ReturnDetail* returnDetail = CreateTransferBatchError(transferBatch, errorCode);

	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*)transferBatch.batchControlInfo->fileSequenceNumber->buf, 
		transferBatch.batchControlInfo->fileSequenceNumber->size);
	//memcpy(&returnDetail->choice.fatalReturn.fileSequenceNumber, transferBatch.batchControlInfo->fileSequenceNumber,
	//	transferBatch.batchControlInfo->fileSequenceNumber->size);
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
	
	ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	return encodeAndUploadRes;
}

int TAPValidator::CreateBatchControlInfoRAPFile(string logMessage, const TransferBatch& transferBatch, int errorCode)
{
	log(LOG_ERROR, "Validation: fileAvailableTimeStamp is missing in Batch Control Info. Creating RAP file.");
	
	log(LOG_ERROR, "Validation: " + logMessage + ". Creating RAP file " + m_rapFilename);
	ReturnDetail* returnDetail = CreateBatchControlInfoError(transferBatch, BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING);
	ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
	int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
	// Unable to call ASN_STRUCT_FREE 'cause we copy pointers from original Transfer Batch to Return Batch structure. 
	// Some memory leak here but not critical 'cause we create one only RAP file at a time.
	// ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	return encodeAndUploadRes;
}


TAPValidationResult TAPValidator::ValidateTransferBatch(const TransferBatch& transferBatch)
{
	// check mandatory structures in Transfer Batch
	if (!transferBatch.batchControlInfo) {
		CreateTransferBatchRAPFile("Batch Control Info missing in Transfer Batch", transferBatch, TF_BATCH_BATCH_CONTROL_INFO_MISSING);
		/*log(LOG_ERROR, "Validation: Batch Control Info missing in Transfer Batch. Creating RAP file " + m_rapFilename);
		ReturnDetail* returnDetail = CreateTransferBatchError(transferBatch, TF_BATCH_BATCH_CONTROL_INFO_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);*/
		return FATAL_ERROR;
	}
	if (!transferBatch.accountingInfo) {
		CreateTransferBatchRAPFile("Accounting Info missing in Transfer Batch", transferBatch, TF_BATCH_ACCOUNTING_INFO_MISSING);

		/*log(LOG_ERROR, "Validation: Accounting Info missing in Transfer Batch. Creating RAP file " + m_rapFilename);
		ReturnDetail* returnDetail = CreateTransferBatchError(transferBatch, TF_BATCH_ACCOUNTING_INFO_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);*/
		return FATAL_ERROR;
	}
	if (!transferBatch.networkInfo) {
		CreateTransferBatchRAPFile("Network Info missing in Transfer Batch", transferBatch, TF_BATCH_NETWORK_INFO_MISSING);

		/*log(LOG_ERROR, "Validation: Network Info missing in Transfer Batch. Creating RAP file " + m_rapFilename);
		ReturnDetail* returnDetail = CreateTransferBatchError(transferBatch, TF_BATCH_NETWORK_INFO_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);*/
		return FATAL_ERROR;
	}
	if (!transferBatch.auditControlInfo) {
		CreateTransferBatchRAPFile("Audit Control Info missing in Transfer Batch", transferBatch, TF_BATCH_AUDIT_CONTROL_INFO_MISSING);

		/*log(LOG_ERROR, "Validation: Audit Control Info missing in Transfer Batch. Creating RAP file " + m_rapFilename);
		ReturnDetail* returnDetail = CreateTransferBatchError(transferBatch, TF_BATCH_AUDIT_CONTROL_INFO_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);*/
		return FATAL_ERROR;
	}
		
	// check mandatory structures in Transfer Batch/Batch Control Information
	if (!transferBatch.batchControlInfo->sender || !transferBatch.batchControlInfo->recipient || !transferBatch.batchControlInfo->fileSequenceNumber) {
		log(LOG_ERROR, "Validation: Sender, Recipient or FileSequenceNumber is missing in Batch Control Info. Unable to create RAP file.");
		return VALIDATION_IMPOSSIBLE;
	}

	if (!transferBatch.batchControlInfo->fileAvailableTimeStamp) {
		log(LOG_ERROR, "Validation: fileAvailableTimeStamp is missing in Batch Control Info. Creating RAP file.");
		ReturnDetail* returnDetail = CreateBatchControlInfoError(transferBatch, BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		// Unable to call ASN_STRUCT_FREE 'cause we copy pointers from original Transfer Batch to Return Batch structure. 
		// Some memory leak here but not critical 'cause we create one only RAP file at a time.
		// ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
		return FATAL_ERROR;
	}
	if (!transferBatch.batchControlInfo->specificationVersionNumber) {
		log(LOG_ERROR, "Validation: specificationVersionNumber is missing in Batch Control Info. Creating RAP file.");
		ReturnDetail* returnDetail = CreateBatchControlInfoError(transferBatch, BATCH_CTRL_SPEC_VERSION_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
		return FATAL_ERROR;
	}
	if (!transferBatch.batchControlInfo->transferCutOffTimeStamp) {
		log(LOG_ERROR, "Validation: transferCutOffTimeStamp is missing in Batch Control Info. Creating RAP file.");
		ReturnDetail* returnDetail = CreateBatchControlInfoError(transferBatch, BATCH_CTRL_TRANSFER_CUTOFF_MISSING);
		ReturnBatch* returnBatch = FillReturnBatch(transferBatch, returnDetail);
		int encodeAndUploadRes = EncodeAndUpload(m_rapFilename, "RAP file", &asn_DEF_ReturnBatch, returnBatch);
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