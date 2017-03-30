package org.heigit.bigspatialdata.hosmdb.osm;

public class OSMMember {
	private final long id;
	private final int type;
	private final int roleId;
	private final Object data;
	
	
	public OSMMember(final long id, final int type, final int roleId){
	  this(id,type, roleId, null);
	}
	
	public OSMMember(final long id, final int type, final int roleId, Object data){
		this.id = id;
		this.type = type;
		this.roleId = roleId;
		this.data = data;
	}
	

	public long getId() {
		return id;
	}
	
	public int getType() {
		return type;
	}

	public int getRoleId() {
		return roleId;
	}
	
	public Object getData(){
	  return data;
	}
	
	@Override
	public String toString() {
		return String.format("T:%d ID:%d R:%d", type,id,roleId);
	}
	
}
