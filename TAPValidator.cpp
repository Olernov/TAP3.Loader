// Class TAPValidator checks TAP file (DataInterchange structure) validity according to TD.57 requirements.
// If Fatal or Severe errors are found it creates RAP file (Return Batch structure) and registers it in DB tables (RAP_File, RAP_Fatal_Return and so on).

#include <math.h>
#include <set>
#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "TAPValidator.h"

using namespace std;

#define NO_ASN_ITEMS	vector<ErrContextAsnItem>()

extern void log(string filename, short msgType, string msgText);
extern void log(short msgType, string msgText);
extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern int LoadReturnBatchToDB(ReturnBatch* returnBatch, long fileID, long roamingHubID, string rapFilename, long fileStatus);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);


RAPFile::RAPFile(otl_connect& dbConnect, Config& config) 
	: m_otlConnect(dbConnect), m_config(config)
{
}


int RAPFile::OctetString_fromInt64(OCTET_STRING& octetStr, long long value)
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


bool RAPFile::UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting)
{
	try {
		if (ftpSetting.ftpPort.length() == 0)
			ftpSetting.ftpPort = "21";	// use default ftp port
		int ncftp_argc = 11;
		const char* pszArguments[] = { "ncftpput", "-u", ftpSetting.ftpUsername.c_str(), "-p", 
			ftpSetting.ftpPassword.c_str(), "-P", ftpSetting.ftpPort.c_str(), ftpSetting.ftpServer.c_str(), 
			ftpSetting.ftpDirectory.c_str(), fullFileName.c_str(), NULL };
		char szFtpResult[4096];
		int ftpResult = ncftp_main(ncftp_argc, (char**) pszArguments, szFtpResult);
		if (ftpResult != 0) {
			log(filename, LOG_ERROR, "Error while uploading file " + filename + " on FTP server " + ftpSetting.ftpServer + ":");
			log(filename, LOG_ERROR, szFtpResult);
			return false;
		}
		log(filename, LOG_INFO, "Successful upload to FTP server " + ftpSetting.ftpServer);
		return true;
	}
	catch (...) {
		log(filename, LOG_ERROR, "Exception while uploading " + filename + " to FTP server " 
			+ ftpSetting.ftpServer + ". Uploading failed.");
		return false;
	}
}


int RAPFile::EncodeAndUpload(ReturnBatch* returnBatch, string filename, string roamingHubName)
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
	asn_enc_rval_t encodeRes = der_encode(&asn_DEF_ReturnBatch, returnBatch, write_out, fTapFile);

	fclose(fTapFile);

	if (encodeRes.encoded == -1) {
		log(filename, LOG_ERROR, string("Error while encoding ASN file. Error code ") + 
			string(encodeRes.failed_type ? encodeRes.failed_type->name : "unknown"));
		return TL_DECODEERROR;
	}

	log(filename, LOG_INFO, "RAP file successfully created for roaming hub " + roamingHubName);

	// Upload file to FTP-server
	FtpSetting ftpSetting = m_config.GetFTPSetting(roamingHubName);
	if (!ftpSetting.ftpServer.empty()) {
		if (!UploadFileToFtp(filename, fullFileName, ftpSetting)) {
			return TL_FILEERROR;
		}
	}
	else
		log(filename, LOG_INFO, "FTP server is not set in cfg-file for roaming hub " + roamingHubName + ". No uploading done.");

	return TL_OK;
}


int RAPFile::CreateRAPFile(ReturnBatch* returnBatch, ReturnDetail* returnDetail, long roamingHubID, string tapSender, string tapRecipient, 
	string tapAvailableStamp, string fileTypeIndicator, long& rapFileID, string& rapSequenceNum)
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.CreateRAPFileByTAPLoader(:pRecipientTAPCode /*char[10],in*/, :pRoamingHubID /*long,in*/, "
		":pTestData /*long,in*/, to_date(:pDate /*char[30],in*/, 'yyyymmddhh24miss'), :pRAPFilename /*char[50],out*/, "
		":pRAPSequenceNum /*char[10],out*/, :pMobileNetworkID /*long,out*/, :pRoamingHubName /*char[100],out*/,"
		":pTimestamp /*char[20],out*/, :pUTCOffset /*char[10],out*/, :pTAPVersion /*long,out*/, :pTAPRelease /*long,out*/, "
		":pRAPVersion /*long,out*/, :pRAPRelease /*long,out*/, :pTapDecimalPlaces /*long,out*/)"
		" into :fileid /*long,out*/" , m_otlConnect);
	otlStream
		<< tapSender
		<< roamingHubID
		<< (long) (fileTypeIndicator.size()>0 ? 1 : 0)
		<< tapAvailableStamp;
	
	string filename, roamingHubName, timeStamp, utcOffset;
	long  mobileNetworkID, tapVersion, tapRelease, rapVersion, rapRelease, tapDecimalPlaces;
	otlStream
		>> filename
		>> rapSequenceNum
		>> mobileNetworkID
		>> roamingHubName
		>> timeStamp
		>> utcOffset
		>> tapVersion
		>> tapRelease
		>> rapVersion
		>> rapRelease
		>> tapDecimalPlaces
		>> rapFileID;
	
	if (rapFileID < 0) {
		log(LOG_ERROR, "TAP3.CreateRAPFileByTAPLoader returned error " + to_string((long long) rapFileID));
		return TL_ORACLEERROR;
	}

	// sender and recipient switch their places
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.sender, tapSender.c_str(), tapSender.size());
	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.recipient, tapRecipient.c_str(), tapRecipient.size());

	OCTET_STRING_fromBuf(&returnBatch->rapBatchControlInfoRap.rapFileSequenceNumber, rapSequenceNum.c_str(), rapSequenceNum.size());
	returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.localTimeStamp =
		OCTET_STRING_new_fromBuf (&asn_DEF_LocalTimeStamp, timeStamp.c_str(), timeStamp.size());
	returnBatch->rapBatchControlInfoRap.rapFileCreationTimeStamp.utcTimeOffset =
		OCTET_STRING_new_fromBuf (&asn_DEF_UtcTimeOffset, utcOffset.c_str(), utcOffset.size());
	returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.localTimeStamp =
		OCTET_STRING_new_fromBuf (&asn_DEF_LocalTimeStamp, timeStamp.c_str(), timeStamp.size());
	returnBatch->rapBatchControlInfoRap.rapFileAvailableTimeStamp.utcTimeOffset =
		OCTET_STRING_new_fromBuf (&asn_DEF_UtcTimeOffset, utcOffset.c_str(), utcOffset.size());

	returnBatch->rapBatchControlInfoRap.tapDecimalPlaces = (TapDecimalPlaces_t*) calloc(1, sizeof(TapDecimalPlaces_t));
	*returnBatch->rapBatchControlInfoRap.tapDecimalPlaces = tapDecimalPlaces;
	
	returnBatch->rapBatchControlInfoRap.rapSpecificationVersionNumber = rapVersion;
	returnBatch->rapBatchControlInfoRap.rapReleaseVersionNumber = rapRelease;
	returnBatch->rapBatchControlInfoRap.specificationVersionNumber = (SpecificationVersionNumber_t*) calloc(1, sizeof(SpecificationVersionNumber_t));
	*returnBatch->rapBatchControlInfoRap.specificationVersionNumber = tapVersion;
	returnBatch->rapBatchControlInfoRap.releaseVersionNumber = (ReleaseVersionNumber_t*) calloc(1, sizeof(ReleaseVersionNumber_t));
	*returnBatch->rapBatchControlInfoRap.releaseVersionNumber = tapRelease;
			
	if (!fileTypeIndicator.empty())
		returnBatch->rapBatchControlInfoRap.fileTypeIndicator = 
			OCTET_STRING_new_fromBuf(&asn_DEF_FileTypeIndicator, fileTypeIndicator.c_str(), fileTypeIndicator.size());
		
// TODO: Operator specific info is mandatory for IOT errors (TD.52 RAP implementation handbook)
// Fill it for Severe errors
			
	ASN_SEQUENCE_ADD(&returnBatch->returnDetails, returnDetail);

	OctetString_fromInt64(returnBatch->rapAuditControlInfo.totalSevereReturnValue, (long long) 0);
	returnBatch->rapAuditControlInfo.returnDetailsCount = 1; // For Fatal errors 

	int loadResult = LoadReturnBatchToDB(returnBatch, rapFileID, roamingHubID, filename, OUTFILE_CREATED_AND_SENT);
	if (loadResult >= 0)
		loadResult = EncodeAndUpload(returnBatch, filename, roamingHubName);
	
	otlStream.close();
	return loadResult;
}

//-----------------------------------------------------------

TAPValidator::TAPValidator(otl_connect& dbConnect, Config& config) 
	: m_otlConnect(dbConnect), m_config(config), m_rapFileID(0)
{
}


bool TAPValidator::IsRecipientCorrect(string recipient)
{
	otl_nocommit_stream otlStream;
	string ourTAPCode;
	otlStream.open(1, "call BILLING.TAP3.GetOurTAPCode(:roamhubid /*long,in*/) into :ourTapCode /*char[20],out*/" , m_otlConnect);
	otlStream << m_roamingHubID;
	otlStream
		>> ourTAPCode;
	otlStream.close();
	return (recipient == ourTAPCode);
}


FileDuplicationCheckRes TAPValidator::IsFileDuplicated()
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.IsTAPFileDuplicated("
		":sender /*char[20],in*/, :recipient /*char[20],in*/, :roam_hub_id /*long,in*/, :file_seqnum /*char[20],in*/, "
		":file_type_indic /*char[20],in*/, :rap_file_seqnum /*char[20],in*/, :notif /*short,in*/, to_date(:avail_stamp /*char[20],in*/,'yyyymmddhh24miss'),"
		":event_count /*long,in*/, :total_charge /*double,in*/ ) "
		"into :res /*long,out*/", m_otlConnect);
	// TODO: check file contents by total event count, notification/transfer batch, total charge etc.
	// If contents are the same just ignore this file. Otherwise create RAP.
	if (m_transferBatch) {
		double tapPower=pow( (double) 10, *m_transferBatch->accountingInfo->tapDecimalPlaces);
		otlStream
			<< m_transferBatch->batchControlInfo->sender->buf
			<< m_transferBatch->batchControlInfo->recipient->buf
			<< m_roamingHubID
			<< m_transferBatch->batchControlInfo->fileSequenceNumber->buf
			<< ( m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : "" )
			<< ( m_transferBatch->batchControlInfo->rapFileSequenceNumber ? (char*) m_transferBatch->batchControlInfo->rapFileSequenceNumber->buf : "" )
			<< (short) 0 /* notification */
			 << m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf
			<< ( m_transferBatch->auditControlInfo->callEventDetailsCount ? *m_transferBatch->auditControlInfo->callEventDetailsCount : 0L )
			<< OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge) / tapPower;
	}
	else {
		otlStream
			<< m_notification->sender->buf
			<< m_notification->recipient->buf
			<< m_roamingHubID
			<< m_notification->fileSequenceNumber->buf
			<< ( m_notification->fileTypeIndicator ? (char*) m_notification->fileTypeIndicator->buf : "" )
			<< ( m_notification->rapFileSequenceNumber ? (char*) m_notification->rapFileSequenceNumber->buf : "" )
			<< (short) 1 /* notification */
			<< m_notification->fileAvailableTimeStamp->localTimeStamp->buf
			<< 0L
			<< 0.0;
	}

	long result;
	otlStream >> result;
	otlStream.close();
	if(result < 0)
	{
		log(LOG_ERROR, "TAP3.IsTAPFileDuplicated returned error " + to_string((long long) result));
		result = DUPLICATION_CHECK_ERROR;
	}
	return (FileDuplicationCheckRes) result;
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


long long TAPValidator::ChargeInfoListTotalCharge(ChargeInformationList* chargeInfoList)
{
	long long totalCharge = 0;
	for (int chr_index = 0; chr_index < chargeInfoList->list.count; chr_index++)
		for (int chr_det_index = 0; chr_det_index < chargeInfoList->list.array[chr_index]->chargeDetailList->list.count; chr_det_index++) {
			if (string((char*) chargeInfoList->list.array[chr_index]->chargeDetailList->list.array[chr_det_index]->chargeType->buf) == "00")
				totalCharge += OctetStr2Int64(*chargeInfoList->list.array[chr_index]->chargeDetailList->list.array[chr_det_index]->charge);
	}
	return totalCharge;
}


long long TAPValidator::BatchTotalCharge()
{
	long long totalCharge = 0;
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++)
				totalCharge += ChargeInfoListTotalCharge(mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList);
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++)
				totalCharge += ChargeInfoListTotalCharge(mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList);
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			totalCharge += ChargeInfoListTotalCharge(mCall->gprsServiceUsed->chargeInformationList);
		}
	}
	return totalCharge;
}


int TAPValidator::CreateBatchControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	log(LOG_ERROR, "Validating Batch Control Info: " + logMessage + ". Creating RAP file ");
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
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_BatchControlInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)
	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.batchControlError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	
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

	return loadRes;
}

TAPValidationResult TAPValidator::FileSequenceNumberControl()
{
	TAPValidationResult res;
	FileDuplicationCheckRes checkRes = IsFileDuplicated();
	int createRapRes;
	vector<ErrContextAsnItem> asnItems;
	switch (checkRes) {
		case DUPLICATION_NOTFOUND:
			return TAP_VALID;
		case COPY_FOUND:
			log(LOG_INFO, "File having same sequence number has already been received and processed. No loading is needed.");
			return FILE_DUPLICATION;
		case DUPLICATION_FOUND:
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
			createRapRes = CreateBatchControlInfoRAPFile(
				"Duplication: File sequence number of the received file has already been received and successfully processed",
				FILE_SEQ_NUM_DUPLICATION, asnItems);
			return ( createRapRes >= 0 ? FILE_DUPLICATION : VALIDATION_IMPOSSIBLE );
		case DUPLICATION_CHECK_ERROR:
			return VALIDATION_IMPOSSIBLE;
	}
}

TAPValidationResult TAPValidator::ValidateBatchControlInfo()
{
	if (!m_transferBatch->batchControlInfo->sender || !m_transferBatch->batchControlInfo->recipient || 
			!m_transferBatch->batchControlInfo->fileSequenceNumber) {
		log(LOG_ERROR, "Validation: Sender, Recipient or FileSequenceNumber is missing in Batch Control Info.");
		return VALIDATION_IMPOSSIBLE;
	}

	if(!IsRecipientCorrect((char*) m_transferBatch->batchControlInfo->recipient->buf)) {
		log(LOG_ERROR, "Validation: Recipient " + string((char*) m_transferBatch->batchControlInfo->recipient->buf) + 
			" is incorrect. File is not addressed for us.");
		return VALIDATION_IMPOSSIBLE;
	}

	if (!m_transferBatch->batchControlInfo->fileAvailableTimeStamp) {
		int createRapRes = CreateBatchControlInfoRAPFile("fileAvailableTimeStamp is missing in Batch Control Info", 
			BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->batchControlInfo->specificationVersionNumber) {
		int createRapRes = CreateBatchControlInfoRAPFile("specificationVersionNumber is missing in Batch Control Info", 
			BATCH_CTRL_SPEC_VERSION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->batchControlInfo->transferCutOffTimeStamp) {
		int createRapRes = CreateBatchControlInfoRAPFile("transferCutOffTimeStamp is missing in Batch Control Info", 
			BATCH_CTRL_TRANSFER_CUTOFF_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	int fileSeqNum;
	try {
		fileSeqNum = stoi((char*)m_transferBatch->batchControlInfo->fileSequenceNumber->buf, NULL);
	}
	catch(const std::invalid_argument& ia) {
		// wrong file sequence number given
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateBatchControlInfoRAPFile("fileSequenceNumber is not a number (syntax error)", 
			FILE_SEQ_NUM_SYNTAX_ERROR, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	
	if (!(fileSeqNum >= START_TAP_SEQUENCE_NUM && fileSeqNum <= END_TAP_SEQUENCE_NUM)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateBatchControlInfoRAPFile("fileSequenceNumber is out of range", 
			FILE_SEQ_NUM_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	return FileSequenceNumberControl();
}


int TAPValidator::CreateAccountingInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	log(LOG_ERROR, "Validating Accounting Info: " + logMessage + ". Creating RAP file");
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
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_AccountingInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	
	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)
	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.accountingInfoError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.currencyConversionInfo = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.discounting = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.localCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.taxation = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);

	return loadRes;
}


TAPValidationResult TAPValidator::ValidateAccountingInfo()
{
	if (!m_transferBatch->accountingInfo->localCurrency) {
		int createRapRes = CreateAccountingInfoRAPFile("localCurrency is missing in Accounting Info", 
			ACCOUNTING_LOCAL_CURRENCY_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->tapDecimalPlaces) {
		int createRapRes = CreateAccountingInfoRAPFile("tapDecimalPlaces is missing in Accounting Info", 
			ACCOUNTING_TAP_DECIMAL_PLACES_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->taxation && BatchContainsTaxes()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"taxation group is missing in Accounting Info and batch contains taxes", 
			ACCOUNTING_TAXATION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->discounting && BatchContainsDiscounts()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"discounting group is missing in Accounting Info and batch contains discounts", 
			ACCOUNTING_DISCOUNTING_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->currencyConversionInfo && BatchContainsPositiveCharges()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"currencyConversion group is missing in Accounting Info and batch contains charges greater than 0",
			ACCOUNTING_CURRENCY_CONVERSION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	// Validating currency conversion table
	if (m_transferBatch->accountingInfo->currencyConversionInfo) {
		set<ExchangeRateCode_t> exchangeRateCodes;
		for (int i = 0; i < m_transferBatch->accountingInfo->currencyConversionInfo->list.count; i++) {
			vector<ErrContextAsnItem> asnItems;
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_CurrencyConversionList, i + 1));
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_EXRATE_CODE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->numberOfDecimalPlaces) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_NUM_OF_DEC_PLACES_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRate) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_EXCHANGE_RATE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check exchange rate code duplication
			if (exchangeRateCodes.find(*m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode) !=
					exchangeRateCodes.end()) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"More than one occurrence of group with same Exchange Rate Code within group Currency Conversion",
					CURRENCY_CONVERSION_EXRATE_CODE_DUPLICATION, asnItems);
				return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
			}
			else {
				exchangeRateCodes.insert(*m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode);
			}
			// check exchange rate code
			// TODO: add checks ?
		}
	}

	if (!(*m_transferBatch->accountingInfo->tapDecimalPlaces >= TAP_DECIMAL_VALID_FROM 
			&& *m_transferBatch->accountingInfo->tapDecimalPlaces <= TAP_DECIMAL_VALID_TO)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_TapDecimalPlaces, 0));
		int createRapRes = CreateAccountingInfoRAPFile("TAP Decimal Places is out of range ( " + 
			to_string((long long) *m_transferBatch->accountingInfo->tapDecimalPlaces) + " value given).", 
			TAP_DECIMAL_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	// Validating taxation table
	if (m_transferBatch->accountingInfo->taxation) {
		set<ExchangeRateCode_t> taxRateCodes;
		for (int i = 0; i < m_transferBatch->accountingInfo->taxation->list.count; i++) {
			vector<ErrContextAsnItem> asnItems;
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxationList, i + 1));
			if (!m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Tax Rate Code missing within group Taxation",
					TAXATION_TAXRATE_CODE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->taxation->list.array[i]->taxType) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Taxation",
					TAXATION_TAX_TYPE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check tax code duplication
			if (taxRateCodes.find(*m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode) !=
					taxRateCodes.end()) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"More than one occurrence of group with same Tax Rate Code within group Taxation",
					CURRENCY_CONVERSION_EXRATE_CODE_DUPLICATION, asnItems);
				return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
			}
			else {
				taxRateCodes.insert(*m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode);
			}
			if (m_transferBatch->accountingInfo->taxation->list.array[i]->taxRate) {
				long taxRate;
				try {
					taxRate = stol((char*) m_transferBatch->accountingInfo->taxation->list.array[i]->taxRate->buf, NULL);
				}
				catch (const std::invalid_argument& ia) {
					// wrong tax rate given
					int createRapRes = CreateAccountingInfoRAPFile("Tax Rate is not a number (syntax error) at element " + to_string((long long) ( i + 1 )),
						TAX_RATE_SYNTAX_ERROR, asnItems);
					return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
				}

				if (!( taxRate >= TAX_RATE_VALID_FROM && taxRate <= TAX_RATE_VALID_TO )) {
					vector<ErrContextAsnItem> asnItems;
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxationList, i + 1));
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_Taxation, 0));
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxRate, 0));
					int createRapRes = CreateAccountingInfoRAPFile("Tax Rate is out of range at element " + to_string((long long) ( i + 1 )),
						TAX_RATE_OUT_OF_RANGE, asnItems);
					return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
				}
			}
		}
	}

	return TAP_VALID;
}


int TAPValidator::CreateNetworkInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	log(LOG_ERROR, "Validating Network Info: " + logMessage + ". Creating RAP file");
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
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_NetworkInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.networkInfoError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.recEntityInfo = NULL;
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.utcTimeOffsetInfo = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);

	return loadRes;
}


TAPValidationResult TAPValidator::ValidateNetworkInfo()
{
	// check mandatory structures in Transfer Batch/Network Information
	if (!m_transferBatch->networkInfo->utcTimeOffsetInfo) {
		int createRapRes = CreateNetworkInfoRAPFile("utcTimeOffsetInfo is missing in Network Info",
			NETWORK_UTC_TIMEOFFSET_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->networkInfo->recEntityInfo) {
		int createRapRes = CreateNetworkInfoRAPFile("recEntityInfo is missing in Network Info",
			NETWORK_REC_ENTITY_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	// Validating recording entities table
	if (m_transferBatch->networkInfo->recEntityInfo) {
		set<RecEntityCode_t> recEntityCodes;
		for (int i = 0; i < m_transferBatch->networkInfo->recEntityInfo->list.count; i++) {
			vector<ErrContextAsnItem> asnItems;
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_RecEntityInfoList, i + 1));
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Code missing within group Network Information",
					REC_ENTITY_CODE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityType) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Type missing within group Network Information",
					REC_ENTITY_TYPE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityId) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Indentification missing within group Network Information",
					REC_ENTITY_IDENTIFICATION_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check recording entity code duplication
			if (recEntityCodes.find(*m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode) !=
					recEntityCodes.end()) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"More than one occurrence of group with same Recording Entity Code within group Recording Entity Information",
					REC_ENTITY_CODE_DUPLICATION, asnItems);
				return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
			}
			else {
				recEntityCodes.insert(*m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode);
			}
		}
	}

	return TAP_VALID;
}


int TAPValidator::CreateAuditControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	log(LOG_ERROR, "Validating Audit Control Info: " + logMessage + ". Creating RAP file");
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.auditControlInfoError = (AuditControlInfoError*) calloc(1, sizeof(AuditControlInfoError));
	
	//Copy auditControlInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.callEventDetailsCount = 
		m_transferBatch->auditControlInfo->callEventDetailsCount;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.earliestCallTimeStamp = 
		m_transferBatch->auditControlInfo->earliestCallTimeStamp;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.latestCallTimeStamp = 
		m_transferBatch->auditControlInfo->latestCallTimeStamp;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.operatorSpecInformation = 
		m_transferBatch->auditControlInfo->operatorSpecInformation;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalAdvisedChargeValueList = 
		m_transferBatch->auditControlInfo->totalAdvisedChargeValueList;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalCharge = 
		m_transferBatch->auditControlInfo->totalCharge;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalChargeRefund = 
		m_transferBatch->auditControlInfo->totalChargeRefund;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountRefund = 
		m_transferBatch->auditControlInfo->totalDiscountRefund;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountValue = 
		m_transferBatch->auditControlInfo->totalDiscountValue;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxRefund = 
		m_transferBatch->auditControlInfo->totalTaxRefund;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxValue = 
		m_transferBatch->auditControlInfo->totalTaxValue;

	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_AuditControlInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	
	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.auditControlInfoError->errorDetail, errorDetail);

	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.callEventDetailsCount = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.earliestCallTimeStamp = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.latestCallTimeStamp = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.operatorSpecInformation = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalAdvisedChargeValueList = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalCharge = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalChargeRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountValue = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxValue = NULL;
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	
	return loadRes;
}


TAPValidationResult TAPValidator::ValidateAuditControlInfo()
{
	// check mandatory structures in Transfer Batch/Audit Control Information
	if (!m_transferBatch->auditControlInfo->totalCharge) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalCharge is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_CHARGE_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->totalTaxValue) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalTaxValue is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_TAX_VALUE_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->totalDiscountValue) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalDiscountValue is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_DISCOUNT_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->callEventDetailsCount) {
		int createRapRes = CreateAuditControlInfoRAPFile("callEventDetailsCount is missing in Audit Control Info",
			AUDIT_CTRL_CALL_COUNT_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (*m_transferBatch->auditControlInfo->callEventDetailsCount != m_transferBatch->callEventDetails->list.count) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_CallEventDetailsCount, 0));
		int createRapRes = CreateAuditControlInfoRAPFile(
			"CallEventDetailsCount does not match the count of Call Event Details",
			CALL_COUNT_MISMATCH, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	long long auditTotalCharge = OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge);
	long long calcTotalCharge = BatchTotalCharge();
	if (OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge) != BatchTotalCharge()) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_TotalCharge, 0));
		int createRapRes = CreateAuditControlInfoRAPFile(
			"Total charge does not match the calculated sum of non refund charges",
			TOTAL_CHARGE_MISMATCH, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	return TAP_VALID;
}


int TAPValidator::CreateTransferBatchRAPFile(string logMessage, int errorCode)
{
	log(LOG_ERROR, "Validating Transfer Batch: " + logMessage + ". Creating RAP file");
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
	
	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	return loadRes;
}


TAPValidationResult TAPValidator::ValidateTransferBatch()
{
	assert(!m_notification);
	assert(m_transferBatch);
	
	// check mandatory structures in Transfer Batch
	if (!m_transferBatch->batchControlInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Batch Control Info missing in Transfer Batch", 
			TF_BATCH_BATCH_CONTROL_INFO_MISSING);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Accounting Info missing in Transfer Batch", 
			TF_BATCH_ACCOUNTING_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->networkInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Network Info missing in Transfer Batch", 
			TF_BATCH_NETWORK_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->auditControlInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Audit Control Info missing in Transfer Batch", 
			TF_BATCH_AUDIT_CONTROL_INFO_MISSING);
		return FATAL_ERROR;
	}
	
	// Validating transfer batch structures
	TAPValidationResult validationRes = ValidateBatchControlInfo();
	if (validationRes != TL_OK)
		return validationRes;

	validationRes = ValidateAccountingInfo();
	if (validationRes != TL_OK)
		return validationRes;

	// check mandatory structures in Transfer Batch/Network Information
	validationRes = ValidateNetworkInfo();
	if (validationRes != TL_OK)
		return validationRes;

	validationRes = ValidateAuditControlInfo();
	if (validationRes != TL_OK)
		return validationRes;
	
	return TAP_VALID;
}


int TAPValidator::CreateNotificationRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	log(LOG_ERROR, "Validating Notification: " + logMessage + ". Creating RAP file");
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_notification->fileSequenceNumber->buf, 
		m_notification->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.notificationError = (NotificationError*) calloc(1, sizeof(NotificationError));
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_Notification.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.notificationError->errorDetail, errorDetail);
	
	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_notification->sender);
	assert(m_notification->recipient);
	assert(m_notification->fileAvailableTimeStamp);
	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_notification->sender->buf,
		(char*) m_notification->recipient->buf, (char*) m_notification->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_notification->fileTypeIndicator ? (char*) m_notification->fileTypeIndicator->buf : ""),
		m_rapFileID, m_rapSequenceNum);
	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
	return loadRes;
}


TAPValidationResult TAPValidator::ValidateNotification()
{
	assert(m_notification);
	assert(!m_transferBatch);
	if (!m_notification->sender || !m_notification->recipient || !m_notification->fileSequenceNumber) {
		log(LOG_ERROR, "Validation: Sender, Recipient or FileSequenceNumber is missing in Notification. ");
		return VALIDATION_IMPOSSIBLE;
	}

	if(!IsRecipientCorrect((char*) m_notification->recipient->buf)) {
		log(LOG_ERROR, "Validation: Recipient " + string((char*) m_notification->recipient->buf) + " is incorrect. ");
		return VALIDATION_IMPOSSIBLE;
	}
	
	int fileSeqNum;
	try {
		fileSeqNum = stoi((char*) m_notification->fileSequenceNumber->buf, NULL);
	}
	catch(const std::invalid_argument& ia) {
		// wrong file sequence number given
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateNotificationRAPFile("fileSequenceNumber is not a number (syntax error)", 
			FILE_SEQ_NUM_SYNTAX_ERROR, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!(fileSeqNum >= START_TAP_SEQUENCE_NUM && fileSeqNum <= END_TAP_SEQUENCE_NUM)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateNotificationRAPFile("fileSequenceNumber is out of range", 
			FILE_SEQ_NUM_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	return FileSequenceNumberControl();
}


TAPValidationResult TAPValidator::Validate(DataInterChange* dataInterchange, long roamingHubID)
{
	m_roamingHubID = roamingHubID;
	switch (dataInterchange->present) {
		case DataInterChange_PR_transferBatch:
			m_transferBatch = &dataInterchange->choice.transferBatch;
			m_notification = NULL;
			return ValidateTransferBatch();
		case DataInterChange_PR_notification:
			m_notification = &dataInterchange->choice.notification;
			m_transferBatch = NULL;
			return ValidateNotification();
		default:
			return VALIDATION_IMPOSSIBLE;
	}
}

long TAPValidator::GetRapFileID()
{
	return m_rapFileID;
}

string TAPValidator::GetRapSequenceNum()
{
	return m_rapSequenceNum;
}