package com.hkbea.cra.batch.job;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hkbea.constant.cra.api.ApiConstants;
import com.hkbea.cra.batch.constants.BatchConstants;
import com.hkbea.cra.batch.dao.CraApiJobDao;
import com.hkbea.cra.batch.model.CraApiCallHttpObj;
import com.hkbea.cra.batch.model.CraApiCatalog;
import com.hkbea.cra.batch.model.CraApiData;
import com.hkbea.cra.batch.model.CraBatchApiEndpoint;
import com.hkbea.cra.batch.util.SQLDataAccess;
import com.hkbea.intf.dao.biz.cra.jdbcdao.CraLoggerDao;
import com.hkbea.model.biz.domain.cra.object.api.response.ApiReturnMessage;
import com.hkbea.model.biz.domain.cra.object.api.response.CraHttpResponse;
import com.hkbea.util.biz.EmailUtility;
import com.hkbea.util.biz.HttpHelper;

public class CraApiCallJob extends CraJob{

	private String rdate;
	
	private String apiId;
	
	private CraBatchApiEndpoint craBatchApiEndpoint;
	
	private CraApiCatalog craApiCatalog;
	
	private String apiBatchLogPath;
	
	private int currentWorker;
	
	private int workerThread;
	
	private String requestMode;
	
	private int totalCase;;
	
	private int successCase;

	private List<String> errorApiDataIdList;
	
	private List<String> errorApiDataIdListAL0001;
	
	private List<String> errorApiDataIdListAL0002;
	
	private List<String> errorApiDataIdListAL0003;
	
	private List<String> errorApiDataIdListAL0004;
	
	private List<String> errorApiDataIdListAL0005;
	
	private List<String> errorApiDataIdListAL0006;
	
	private List<String> errorApiDataIdListAL0007;

	private List<String> longRunCustomerList;
	
	// Report Use
	private String apiReportPath;
	
	private String successReportFileName;
	
	private String exceptionReportFileName;
	
	private HashMap<String, String> successResponses;
	
	private HashMap<String, String> failResponses;
	
	private List<CraApiCallHttpObj> successApiCall;
	
	private List<CraApiCallHttpObj> failApiCall;
	
	private CraApiJobDao craApiJobDao;

	private Connection conn;

	public CraApiCallJob(String rdate, String apiId, String apiBatchLogPath, String apiReportPath, int workerThread, String requestMode){
		
		this.rdate = rdate;
		this.apiId = apiId;
		this.apiBatchLogPath = apiBatchLogPath;
		this.workerThread = workerThread;
		this.currentWorker = 0;
		this.requestMode = requestMode;
		
		// Email Handling
		this.totalCase = 0;
		this.successCase = 0;
		this.errorApiDataIdList = new ArrayList<String>();
		this.errorApiDataIdListAL0001 = new ArrayList<String>();
		this.errorApiDataIdListAL0002 = new ArrayList<String>();
		this.errorApiDataIdListAL0003 = new ArrayList<String>();
		this.errorApiDataIdListAL0004 = new ArrayList<String>();
		this.errorApiDataIdListAL0005 = new ArrayList<String>();
		this.errorApiDataIdListAL0006 = new ArrayList<String>();
		this.errorApiDataIdListAL0007 = new ArrayList<String>();
		this.longRunCustomerList = new ArrayList<String>();
		// Report Handling
		this.apiReportPath = apiReportPath;
		this.successResponses = new HashMap<String, String>();
		this.failResponses = new HashMap<String, String>();
		
		this.successApiCall = new ArrayList<CraApiCallHttpObj>();
		this.failApiCall = new ArrayList<CraApiCallHttpObj>();
		
		this.craApiJobDao = new CraApiJobDao();
	}
	
	@Override
	public boolean preExecute(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate) {
		
		// 1. Create Database Access
		try{
			SQLDataAccess da = new SQLDataAccess();
			this.conn = da.OpenConnection();
			this.conn.setAutoCommit(false);
		} catch (SQLException ex){
			ex.printStackTrace();
			this.closeConnection();
			return false;
		}
		
		// 2. Log Path Parsing
		if (!this.apiBatchLogPath.endsWith("\\")){
			this.apiBatchLogPath += "\\";
		}
		this.apiBatchLogPath += this.rdate + "\\";
		
		if (!this.apiReportPath.endsWith("\\")){
			this.apiReportPath += "\\";
		}
		this.apiReportPath += this.rdate + "\\";
		
		// 3. Create Path if not exist
		File logPath = new File(this.apiBatchLogPath);
		if (!logPath.exists()){
			logPath.mkdirs();
		}
		File reportPath = new File(this.apiReportPath);
		if (!reportPath.exists()){
			reportPath.mkdirs();
		}
		
		return true;
	}

	@Override
	public boolean onExecuting(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate) throws SQLException {
		
		//this.craLoggerJdbcDao.logToDB("ST Test3", "Test 1" , "Test 2", "Test3 ", "Test 4" ,"home", "Test5", "TEMPNA");
		
		// 1.1. Retrieve Corresponding API Endpoint & Request Body
		this.craBatchApiEndpoint = this.craApiJobDao.retrieveBatchCallApi(conn, this.apiId);
		
		// 1.2. Retrieve API Catalog for API Name => 
		this.craApiCatalog = this.craApiJobDao.retrieveCatalog(conn, this.apiId);
		
		if (this.craBatchApiEndpoint == null){
			this.setRecentError("CRA Batch Endpoint cannot be retrieved");
			return false;
		}
		
		if (this.craApiCatalog == null){
			this.setRecentError("CRA Api Catalog cannot be retrieved");
			return false;
		}
		
		// 1.3. Allocate Report Name
		this.successReportFileName = this.rdate + "_" + this.craApiCatalog.getApiDataName() + "_Complete_Report.csv";
		this.exceptionReportFileName = this.rdate + "_" + this.craApiCatalog.getApiDataName() + "_Exception_Report.csv";
		
		// 1.4. Retrieve Relevant Request Body And Api Log Prefix and provide the parameter map
		final HashMap<String, String> paramsList = new HashMap<String, String>();
		final HashMap<String, String> logParamsList = new HashMap<String, String>();
		final String requestBody = this.craBatchApiEndpoint.getApiRequestBody();
		final String logPrefix = this.craBatchApiEndpoint.getApiLogPrefix();
		
		Pattern substitutePattern = Pattern.compile("%(\\d+)%");
		Matcher bodyMatcher = substitutePattern.matcher(requestBody);
		Matcher logMatcher = substitutePattern.matcher(logPrefix);
		
		while (bodyMatcher.find()){
			if (!paramsList.containsKey(bodyMatcher.group(1))){
				paramsList.put(bodyMatcher.group(1), bodyMatcher.group(0));
			}
		}
		
		while (logMatcher.find()){
			if (!logParamsList.containsKey(logMatcher.group(1))){
				logParamsList.put(logMatcher.group(1), logMatcher.group(0));
			}
		}
		
		// 1.5. Add %RDATE%
		requestBody.replace("%RDATE%", this.rdate);
		logPrefix.replace("%RDATE%", this.rdate);
		
		// 2. Retrieve Data
		List<CraApiData> craApiData = this.craApiJobDao.retrieveApiData(conn, this.apiId, this.rdate);
		this.totalCase = craApiData.size();
		
		// 3. Initialize ExecutorService
		ExecutorService executor = Executors.newFixedThreadPool(this.workerThread);
		List<Future<?>> tasks = new ArrayList<>();
		
		for (final CraApiData cad : craApiData){

			try {
				this.craApiJobDao.updateApiDataCalledFlag(conn,cad.getApiDataId() , "Y");
				conn.commit();

			Future<?> future = executor.submit(new Runnable(){

				@Override
				public void run() {
					try{
						
						int fetchRetryTimes = 0;
						CraApiCallHttpObj craApiCallHttpObj = new CraApiCallHttpObj(cad);
						
						String filledRequestBody = requestBody;
						for (Entry<String, String> pair : paramsList.entrySet()){
							//Object value = CraApiData.class.getDeclaredMethod("getApiVal"+pair.getKey()).invoke(cad);
							Object value = craApiCallHttpObj.getApiVal(pair.getKey());
							filledRequestBody = filledRequestBody.replace(pair.getValue(), value == null ? "" : (String) value);
						}
						
						//Added by Spencer 241030 for joining TBL_CRA_STRAIGHT_THROUGH_RESULT & TBL_API_RES_DATA
						if(filledRequestBody.contains("api_data_id"))
						{
							filledRequestBody = filledRequestBody.replace("apiDataID", cad.getApiDataId());
						}						

						craApiCallHttpObj.setRequestBody(filledRequestBody);
						
						Calendar beforePostTs =  Calendar.getInstance();
						//CraHttpResponse response = HttpHelper.executeJsonPost(craBatchApiEndpoint.getApiEndpoint(), filledRequestBody, 3);
						String endPointUrl = craBatchApiEndpoint.getApiEndpoint();
						CraHttpResponse response = null;
						
						//Added by Spencer for loging
				        LocalDateTime now = LocalDateTime.now();
				        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				        String formattedDateTime = now.format(formatter);
						System.out.println(formattedDateTime + ":Data ID =" + cad.getApiDataId() + " : . onExecuting");
						Thread.sleep(2000);
						now = LocalDateTime.now();
					    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					    formattedDateTime = now.format(formatter);
				        System.out.println(formattedDateTime + ":Data ID =" + cad.getApiDataId() + " : After sleep");
 
						if (requestMode.equals(BatchConstants.REQUEST_MODE_DIRECT)){
							craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Data ID =" + cad.getApiDataId() + " : Direct Request.", "CraApiCallJob","onExecuting", null, null);
							response = HttpHelper.executeJsonPost(craBatchApiEndpoint.getApiEndpoint(), filledRequestBody, 0);
						}else {
							craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Data ID =" + cad.getApiDataId() + " : Queue Request.", "CraApiCallJob","onExecuting", null, null);
							String queueEndPoint = endPointUrl + (endPointUrl.endsWith("/") ? "" : "/") + "Queue";
							CraHttpResponse queueResponse = HttpHelper.executeJsonPost(queueEndPoint, filledRequestBody, 0);
							craApiCallHttpObj.setResponseBody(queueResponse.getResponseBody());
							
							if (queueResponse.getResponseCode() == HttpStatus.SC_CREATED){
			
								//String requestKey = retrieveBodyMessage(new String[]{"response" ,"ret_obj"}, null, queueResponse.getResponseBody(), null);
								//String fetchEndPint = endPointUrl + (endPointUrl.endsWith("/") ? "" : "/") + "Fetch/" + requestKey;
								String requestKey = retrieveBodyMessage(new String[]{"response" ,"ret_obj"}, craApiCallHttpObj);
								String fetchEndPoint = queueResponse.getLocation();

								boolean waitProcess = true;
								do{
									craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Data ID =" + cad.getApiDataId() + " : Fetch Request.", "CraApiCallJob","onExecuting", null, null);
									CraHttpResponse fetchResponse = HttpHelper.executeJsonPost(fetchEndPoint, "", 0);
									//if (fetchResponse.getResponseCode() == HttpStatus.SC_ACCEPTED){
									if (fetchResponse.getResponseCode() == HttpStatus.SC_ACCEPTED && fetchRetryTimes < 2000){
										System.out.println("Data ID = " + cad.getApiDataId() + " : Wait Fetch Request.");
										fetchRetryTimes++;
										Thread.sleep(3000);
									}else{
										craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Data ID =" + cad.getApiDataId() + " : Complete Fetch Request.", "CraApiCallJob","onExecuting", null, null);
										response = fetchResponse;
										waitProcess = false;
									}
								}while (waitProcess);
							}else{
								response = queueResponse;
							}
						}
						
						craApiCallHttpObj.setResponseBody(response.getResponseBody());
						
						Calendar afterPostTs = Calendar.getInstance();
						
						// Log Handling 						
						String outputLogName = String.format("%03d", craBatchApiEndpoint.getApiId()) + "_" + logPrefix;
						for (Entry<String, String> pair : logParamsList.entrySet()){
							Object value = CraApiData.class.getDeclaredMethod("getApiVal"+pair.getKey()).invoke(cad);
							outputLogName = outputLogName.replace(pair.getValue(), value == null ? "" : (String) value);
						}
						outputLogName = outputLogName.replace("%RDATE%", rdate);
						outputLogName = outputLogName + "POST.log";
						
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
						
						PrintWriter writer = new PrintWriter(new File(apiBatchLogPath + outputLogName));
						writer.write(filledRequestBody + "\r\n");
						writer.write(response.getResponseCode() + "\r\n");
						writer.write(response.getResponseBody() + "\r\n");
						writer.write(sdf.format(beforePostTs.getTime()) + "\r\n");
						writer.write(sdf.format(afterPostTs.getTime()) + "\r\n");
						writer.write((afterPostTs.getTimeInMillis() - beforePostTs.getTimeInMillis()) + "ms\r\n");
						writer.close();
						
						//Long run handling
						if(fetchRetryTimes >2000)
						{
							longRunCustomerList.add(cad.getApiVal1());
						}
						else
						{
							// Report & Email Handling
							if (response.getResponseCode() == HttpStatus.SC_OK){
								ObjectMapper objectMapper = new ObjectMapper();
								ApiReturnMessage apiReturnMessage = objectMapper.readValue(response.getResponseBody(), ApiReturnMessage.class);
								if(StringUtils.isNotEmpty(apiReturnMessage.getRetMessage())){
								
								if(apiReturnMessage.getRetMessage().contains(ApiConstants.NO_PE3_RECORDS)){
									errorApiDataIdListAL0001.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.PE3_OPR_ERROR)){
									errorApiDataIdListAL0002.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.CURRENT_MEDIUM_RR)){
									errorApiDataIdListAL0003.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.CURRENT_HIGH_RR)){
									errorApiDataIdListAL0004.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.PE3_INPUT_ERROR)){
									errorApiDataIdListAL0005.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.IS_PBKD_CUS)){
									errorApiDataIdListAL0006.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else if(apiReturnMessage.getRetMessage().contains(ApiConstants.IS_Dormant_CUS)){
									errorApiDataIdListAL0007.add(cad.getApiVal1());
									failResponses.put(filledRequestBody, response.getResponseBody());
									failApiCall.add(craApiCallHttpObj);
								}else{
									successResponses.put(filledRequestBody, response.getResponseBody());
									successApiCall.add(craApiCallHttpObj);
									successCase++;
								}
							}
		
							}else{
								failResponses.put(filledRequestBody, response.getResponseBody());
								failApiCall.add(craApiCallHttpObj);
								//errorApiDataIdList.add(cad.getApiDataId());
								errorApiDataIdList.add(cad.getApiVal1());
							}
						}
						
					} catch (Exception e){
						//errorApiDataIdList.add(cad.getApiDataId());
						errorApiDataIdList.add(cad.getApiVal1());
						e.printStackTrace();
					} finally {
						currentWorker--;
					}
				}
			});

			tasks.add(future);
			
			} catch (Exception ex) {
				//craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Data ID =" + cad.getApiDataId() + " " + ex.getMessage() , "CraApiCallJob","onExecuting", null, null);
			}
			finally
			{
				//conn.commit();
			}
		}
		
		int pending = this.totalCase;
		
		do{
			pending = 0;
			for (Future<?> task : tasks){
				if (!task.isDone()){
					pending++;
				}
			}
			craLoggerJdbcDao.logToDB("",CraLoggerDao.batchTask, CraLoggerDao.progress, "Pending Execution Thread = " + pending, "CraApiCallJob","onExecuting", null, null);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} while (pending > 0);
		
		executor.shutdown();
		
		return true;
	}

	@Override
	public boolean postExecute(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate) {

		// 1. Close DB Connection
		this.closeConnection();
		
		// 2. Generate Report
		boolean reportGenerated = this.generateReport();
		if (!reportGenerated){
			return false;
		}
				
		// 3. Send Result Email to CRA Application Team
		String result = "Success";
		if (this.errorApiDataIdList.size() > 0){
			result = "Complete with Alert";
		}
		
		String emailSubject = this.retrieveEmailSubject();
		String emailContent = this.retrieveItdEmailContent();
		try
		{
			EmailUtility.SendEmailWithAttachments(this.smtpHost, this.smtpPort, this.smtpSender, new String[]{this.itdEmail}, new String[]{}, new String[]{}, emailSubject, emailContent, new String[]{});
		}
		catch (Exception e){
			e.printStackTrace();
			this.setRecentError("ITD Email Failed");
			return false;
		}
		
		// 4. Send Result Email to User
		String userEmailSubject = "[ECR] " + this.rdate + " - Summary Report for Straight Through Result of [" + this.craApiCatalog.getApiDpName() + "]";
		String userEmailContent = this.retrieveUserEmailContent();
		try
		{
			EmailUtility.SendEmailWithAttachments(this.smtpHost, this.smtpPort, this.smtpSender, this.craBatchApiEndpoint.getApiReportRecipients().split(";"), new String[]{}, new String[]{}, userEmailSubject, userEmailContent, new String[]{});
		}
		catch (Exception e){
			e.printStackTrace();
			this.setRecentError("User Email Failed");
			return false;
		}
		
		return true;
	}
	
	private boolean generateReport(){
		
		// 1. Initialize StringBuilder
		StringBuilder successReportBuilder = new StringBuilder();
		StringBuilder exceptionReportBuilder = new StringBuilder();
		
		// 2. Add Header
		successReportBuilder.append(this.craBatchApiEndpoint.getApiReportHeader() + "\r\n");
		exceptionReportBuilder.append(this.craBatchApiEndpoint.getApiReportHeader() + "\r\n");
		String reportContent = this.craBatchApiEndpoint.getApiReportContent();
		
		// 3. Pattern retrieval
		Pattern substitutePattern = Pattern.compile("%([^%]+)%");
		Matcher contentMatcher = substitutePattern.matcher(reportContent);
		HashMap<String, String> paramList = new HashMap<String, String>();
		
		while (contentMatcher.find()){
			paramList.put(contentMatcher.group(0), contentMatcher.group(1));
		}
		
		// 4. Append For Each successful Executed Entry
		try{
			for (CraApiCallHttpObj sCase : this.successApiCall){
				
				String retCode = this.retrieveBodyMessage(new String[]{"response" ,"ret_code"}, sCase);
				
				if (retCode.equals(BatchConstants.API_RESULT_OK)){
					successReportBuilder.append(this.retrieveReportContent(reportContent, paramList, sCase) + "\r\n");
				}else{
					exceptionReportBuilder.append(this.retrieveReportContent(reportContent, paramList, sCase) + "\r\n");
				}
				
			}
		} catch (Exception e){
			e.printStackTrace();
			this.setRecentError("JSON/IO Exception when parsing successful case");
			return false;
		}
		
		// 5. Write to File 
		try{
			PrintWriter writer = new PrintWriter(new File(this.apiReportPath + this.successReportFileName));
			writer.write(successReportBuilder.toString());
			writer.close();
			
			PrintWriter writer2 = new PrintWriter(new File(this.apiReportPath + this.exceptionReportFileName));
			writer2.write(exceptionReportBuilder.toString());
			writer2.close();

		} catch (Exception e){
			e.printStackTrace();
			this.setRecentError("Report Write Error");
			return false;
		}
		
		return true;
	}
	
	private String retrieveReportContent(String content, HashMap<String, String> paramList, CraApiCallHttpObj craApiCallHttpObj) throws Exception{
		
		String parsedContent = content;
		for (Entry<String, String> param : paramList.entrySet()){
			
			String[] allParameters = param.getValue().split("\\|");
			String retrievedVal = "";
			
			for (int i = 0; i < allParameters.length; i++){
				String[] paramHierachy = allParameters[i].split("\\.");
				retrievedVal = this.retrieveBodyMessage(paramHierachy, craApiCallHttpObj);
				if (retrievedVal != null && !retrievedVal.equals("")){
					break;
				}
			}
			parsedContent = parsedContent.replace(param.getKey(), retrievedVal.replace("\"", "\"\""));
		}
		
		return parsedContent;
	}
	
	private String retrieveBodyMessage(String[] paramHierachy, CraApiCallHttpObj craApiCallHttpObj) throws Exception{
		
		ObjectMapper om = new ObjectMapper();
		
		JsonNode rootNode = null;
		JsonNode currentNode = null;

		if (paramHierachy[0].equals("request")){
			rootNode = new ObjectMapper().readTree(craApiCallHttpObj.getRequestBody());
			currentNode = rootNode;
		}else if (paramHierachy[0].equals("response")){
			rootNode = new ObjectMapper().readTree(craApiCallHttpObj.getResponseBody());
			currentNode = rootNode;
		}else if (paramHierachy[0].toLowerCase().startsWith("apival")){	
			if(paramHierachy != null) {
				return craApiCallHttpObj.getApiVal(paramHierachy[0].substring(6)).toString();
			}else {
				return "";
			}
		}else{
			return "";
		}

		
		for (int i = 1; i < paramHierachy.length; i++){
			currentNode = currentNode.get(paramHierachy[i]);
			if (currentNode == null){
				return "";
			}
		}
		
		return currentNode.asText();
	}
	/*
	private String retrieveReportContent(String content, HashMap<String, String> paramList, String request, String response) throws JsonProcessingException, IOException{
		
		String parsedContent = content;
		for (Entry<String, String> param : paramList.entrySet()){
			
			String[] allParameters = param.getValue().split("\\|");
			String retrievedVal = "";
			
			for (int i = 0; i < allParameters.length; i++){
				String[] paramHierachy = allParameters[i].split("\\.");
				retrievedVal = this.retrieveBodyMessage(paramHierachy, request, response, supplement);
				if (retrievedVal != null && !retrievedVal.equals("")){
					break;
				}
			}
			parsedContent = parsedContent.replace(param.getKey(), retrievedVal.replace("\"", "\"\""));
		}
		
		return parsedContent;
	}
	
	private String retrieveBodyMessage(String[] paramHierachy, String request, String response, String supplement) throws JsonProcessingException, IOException{
			
		ObjectMapper om = new ObjectMapper();
		
		JsonNode rootNode = null;
		JsonNode currentNode = null;

		if (paramHierachy[0].equals("request")){
			rootNode = new ObjectMapper().readTree(request);
			currentNode = rootNode;
		}else if (paramHierachy[0].equals("response")){
			rootNode = new ObjectMapper().readTree(response);
			currentNode = rootNode;
		}else if (paramHierachy[0].equals("apiVal")){	
			rootNode = new ObjectMapper().readTree(supplement);
			currentNode = rootNode;
		}else{
			return "";
		}

		
		for (int i = 1; i < paramHierachy.length; i++){
			currentNode = currentNode.get(paramHierachy[i]);
			if (currentNode == null){
				return "";
			}
		}
		
		return currentNode.asText();
	}
	*/
	
	private String retrieveItdEmailContent(){
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("Dear All, \r\n\r\n");
		//sb.append("Execution of API Call (API ID=" + this.apiId + ") with RDATE = " + this.rdate + " is completed.\r\n");
		sb.append("Execution of API Batch Call (API Name= "+ this.craApiCatalog.getApiDpName() +" API ID=" + this.apiId + ") with RDATE = " + this.rdate + " is completed.\r\n");
		sb.append("Total Case\t: " + this.totalCase + "\r\n");
		sb.append("Executed Case\t: " + this.successCase + "\r\n");
		sb.append("Error Case\t: " + this.errorApiDataIdList.size() + "\r\n\r\n");

		if (this.errorApiDataIdList.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Please investigate following Error Cases (Internal Customer ID): \r\n");
			for (String apiDataId : this.errorApiDataIdList){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0001.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0001): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0001){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0002.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0002): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0002){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0003.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0003): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0003){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0004.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0004): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0004){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0005.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0005): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0005){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0006.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0006): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0006){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		if (this.errorApiDataIdListAL0007.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("Following customer could not be found in Alnova (Error message AL0007): \r\n");
			for (String apiDataId : this.errorApiDataIdListAL0007){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		
		sb.append("Possible Long Run Case\t: " + this.longRunCustomerList.size() + "\r\n\r\n");
		if (this.longRunCustomerList.size() > 0){
			//sb.append("Please investigate following Error Cases (API Data ID): \r\n");
			sb.append("The following Cases may be long running (Internal Customer ID): \r\n");
			for (String apiDataId : this.longRunCustomerList){
				sb.append(apiDataId + "\r\n");
			}
			sb.append("\r\n");
		}
		
		sb.append("Best Regards, \r\nITD-CRA-Application Team");
		
		return sb.toString();
		
	}
	
	private String retrieveUserEmailContent(){
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("Dear All, \r\n\r\n");
		sb.append("Straight Through Approval for [" + this.craApiCatalog.getApiDpName() + "] is complete, report(s) will be delivered to SFTP folder for your access.");
		sb.append("\r\n\r\n");
		sb.append("Best Regards, \r\nITD-CRA-Application Team");
		
		return sb.toString();
	}
	
	public void closeConnection(){
		try {
			if (this.conn != null && !this.conn.isClosed()){
				this.conn.close();
			}
		} catch (SQLException e) {

		}
	}
	
	private String getJsonEscapedString(String orgValue){
		return orgValue.replace("\\", "\\\\").replace("\"", "\\\"");
	}
	
	private String retrieveEmailSubject()
	{
		return "[CRA] Batch " + this.craApiCatalog.getApiDpName() + " - API Call (ID = " + this.apiId + ") (RDATE = " + this.rdate + ")";
	}

}
