package com.hkbea.cra.batch.job;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.hkbea.cra.batch.intf.CraEmailAction;
import com.hkbea.cra.batch.intf.CraZipAction;
import com.hkbea.dao.biz.cra.jdbcdao.CraLoggerJdbcDao;
import com.hkbea.intf.service.biz.cra.dao.CraLoggerService;
import com.hkbea.model.biz.util.CraTypeUtil;

public abstract class CraJob implements CraEmailAction, CraZipAction{

	protected final String settingPath = "file:D:/CRA/Batch/config/Spring.xml"; 

	protected final String settingFileName;
	
	// SMTP
	protected String smtpHost;
	
	protected String smtpPort;
	
	protected String smtpSender;
	
	protected String itdEmail;
	
	// 7zip
	protected String zipProgramPath;
	
	public enum CraJobStatus {
		PreExecuteFail,
		ExecutingFail,
		PostExecuteFail,
		UnexpectedFail,
		Success
	}

	private String recentError;
	
	public CraLoggerJdbcDao craLoggerJdbcDao;
	public CraLoggerService craLoggerService;
	protected ApplicationContext context;
	
	
	public CraJob(){
		//Added by Spencer
		context  = new FileSystemXmlApplicationContext(settingPath);
	    craLoggerJdbcDao = context.getBean(CraLoggerJdbcDao.class);
		this.settingFileName = this.getClass().getSimpleName();
	};
	
	public abstract boolean preExecute(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate);
	
	public abstract boolean onExecuting(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate) throws Exception;
	
	public abstract boolean postExecute(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate);
	
	public CraJobStatus execute(String dept_code, String is_Add_Hoc, String dept_code_group, String rDate) throws Exception{
		
		try{			
			if (!this.preExecute(dept_code, is_Add_Hoc, dept_code_group, rDate)){
				this.printError();
				return CraJobStatus.PreExecuteFail;
			}else if (!this.onExecuting(dept_code, is_Add_Hoc, dept_code_group, rDate)){
				this.printError();
				return CraJobStatus.ExecutingFail;
			}else if (!this.postExecute(dept_code, is_Add_Hoc, dept_code_group, rDate)){
				this.printError();
				return CraJobStatus.PostExecuteFail;
			}
			
			return CraJobStatus.Success;
		}
		catch (Exception ex){
			//System.out.println(ex.getStackTrace()[0]);
			ex.printStackTrace();
			return CraJobStatus.UnexpectedFail;
		}
		
	}

	@Override
	public void assignEmailParameter(String smtpHost, String smtpPort, String smtpSender, String itdEmail) {
		this.smtpHost = smtpHost;
		this.smtpPort = smtpPort;
		this.smtpSender = smtpSender;
		this.itdEmail = itdEmail;
	}

	
	@Override
	public void assign7ZipPath(String programPath) {
		this.zipProgramPath = programPath;
	}
	
	private void printError(){
		if (!CraTypeUtil.isNullOrEmpty(this.recentError)){
			System.out.println("Error is occured during execution : " + this.recentError);
		}
	}
	
	public String getRecentError() {
		return recentError;
	}

	public void setRecentError(String recentError) {
		this.recentError = recentError;
	}
	
}
