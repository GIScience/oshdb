package org.heigit.bigspatialdata.oshpbf.osm;

import java.util.Arrays;
import java.util.List;

public class OSMPbfRelation extends OSMPbfEntity {
	public static class OSMMember implements Comparable<OSMMember> {

		private final long memId;
		private final String role;
		private final int type;

		public OSMMember(long memId, String role, int type) {
			this.memId = memId;
			this.role = role;
			this.type = type;
		}

		public long getMemId() {
			return memId;
		}

		public int getType() {
			return type;
		}

		public String getRole() {
			return role;
		}

		@Override
		public String toString() {
			return String.format("%d T:%d [%s]", memId, type, role);
		}

		@Override
		public int compareTo(OSMMember o) {
			int c = Long.compare(memId, o.memId);
			if (c == 0) {
				c = Integer.compare(type, o.type);
				if (c == 0) {
					if(role == null && o.role == null)
						return 0;
					if (role == null)
						return -1;
					if (o.role == null)
						return 1;
					c = role.compareTo(o.role);
				}
			}
			return c;
		}
		
		@Override
		public int hashCode() {
			int result = 425;
			result = 37*result + (int)memId;
			result = 37*result + type;
			result = 37*result + role.hashCode();
		
			return result;
		}
	}

	private final List<OSMMember> members;

	public OSMPbfRelation(OSMCommonProperties props, List<OSMMember> members) {
		super(props);
		this.members = members;
	}

	public List<OSMMember> getMembers() {
		return this.members;
	}

	@Override
	public String toString() {
		return String.format("R-> %s\n\tMembers: %s", super.toString(), Arrays.toString(members.toArray()));
	}

	@Override
	public Type getType() {
		return Type.RELATION;
	}

}