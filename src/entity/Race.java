package entity;

public class Race {
	private String SP_ID;
	private String SERVICE_ID;
	private String CLASS_ID;
	private int RATE;
	private int PER_UNITCOUNT;
	
	public String getSP_ID() {
		return SP_ID;
	}
	public void setSP_ID(String sP_ID) {
		SP_ID = sP_ID;
	}
	public String getSERVICE_ID() {
		return SERVICE_ID;
	}
	public void setSERVICE_ID(String sERVICE_ID) {
		SERVICE_ID = sERVICE_ID;
	}
	public String getCLASS_ID() {
		return CLASS_ID;
	}
	public void setCLASS_ID(String cLASS_ID) {
		CLASS_ID = cLASS_ID;
	}
	public int getRATE() {
		return RATE;
	}
	public void setRATE(int rATE) {
		RATE = rATE;
	}
	public int getPER_UNITCOUNT() {
		return PER_UNITCOUNT;
	}
	public void setPER_UNITCOUNT(int pER_UNITCOUNT) {
		PER_UNITCOUNT = pER_UNITCOUNT;
	}

}
