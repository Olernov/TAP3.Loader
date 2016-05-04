#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "ConfigContainer.h"
#include "ReturnBatch.h"
#include "RAPFile.h"

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
			log(filename, LOG_ERROR, string("Error while uploading file ") + filename + " on FTP server " + ftpSetting.ftpServer + ":");
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

