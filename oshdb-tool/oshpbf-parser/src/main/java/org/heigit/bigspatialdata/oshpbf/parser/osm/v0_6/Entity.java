package org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public abstract class Entity {
	public final CommonEntityData entityData;

	public Entity(final CommonEntityData entityData) {
		this.entityData = entityData;
	}

	public long getId() {
		return entityData.id;
	}

	public int getVersion() {
		return entityData.version;
	}

	public long getTimestamp() {
		return entityData.timestamp;
	}

	public long getChangeset() {
		return entityData.changeset;
	}

	public int getUserId() {
		return entityData.userId;
	}

	public String getUser() {
		return entityData.user;
	}

	public boolean isVisible() {
		return entityData.visible;
	}

	public TagText[] getTags() {
		return entityData.tags;
	}

	@Override
	public String toString() {
	  return entityData.toString();
	}
	
	public abstract OSMType getType();
}
