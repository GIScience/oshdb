package org.heigit.bigspatialdata.hosmdb.osm;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;

/** Holds an OSH-Object that belongs to the Way or Relation this Member is contained in.
 *
 * @author Rafael Troilo <r.troilo@uni-heidelberg.de>
 */
public class OSMMember {
	private final long id;
	private final int type;
	private final int roleId;
	private final HOSMEntity entity;

	
	public OSMMember(final long id, final int type, final int roleId){
	  this(id,type, roleId, null);
	}
	
	public OSMMember(final long id, final int type, final int roleId, HOSMEntity entity){
		this.id = id;
		this.type = type;
		this.roleId = roleId;
		this.entity = entity;
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
	
	public HOSMEntity getEntity(){
	  return entity;
	}
	
	@Override
	public String toString() {
		return String.format("T:%d ID:%d R:%d", type,id,roleId);
	}
	
}
