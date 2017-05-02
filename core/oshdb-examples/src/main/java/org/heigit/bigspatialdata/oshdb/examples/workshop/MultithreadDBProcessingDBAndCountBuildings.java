package org.heigit.bigspatialdata.oshdb.examples.workshop;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;

public class MultithreadDBProcessingDBAndCountBuildings {

	public static class ZoomId {
		public final int zoom;
		public final long id;

		public ZoomId(final int zoom, final long id) {
			this.zoom = zoom;
			this.id = id;
		}
	}

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, IOException, org.json.simple.parser.ParseException {
		Class.forName("org.h2.Driver");
		// set path to DB
		try (Connection conn = DriverManager.getConnection("jdbc:h2:D:/heidelberg-ccbysa", "sa", "");
				final Statement stmt = conn.createStatement()) {

			System.out.println("Select tag key/value ids from DB");
			ResultSet rstTags = stmt.executeQuery(
					"select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
			Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
			while (rstTags.next()) {
				int keyId = rstTags.getInt(1);
				int valueId = rstTags.getInt(2);
				String keyStr = rstTags.getString(3);
				String valueStr = rstTags.getString(4);
				if (!allKeyValues.containsKey(keyStr))
					allKeyValues.put(keyStr, new HashMap<>());
				allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
			}
			rstTags.close();

			ResultSet rstRoles = stmt.executeQuery("select ID as ROLEID, txt as ROLE from ROLE;");
			Map<String, Integer> allRoles = new HashMap<>();
			while (rstRoles.next()) {
				int roleId = rstRoles.getInt(1);
				String roleStr = rstRoles.getString(2);
				allRoles.put(roleStr, roleId);
			}
			rstRoles.close();

			List<ZoomId> zoomIds = new ArrayList<>();
			System.out.println("Select ids from DB");
			ResultSet rst = stmt.executeQuery("select level,id from grid_way");
			while (rst.next()) {
				zoomIds.add(new ZoomId(rst.getInt(1), rst.getLong(2)));
			}
			rst.close();

			List<Long> timestamps;
			timestamps = new ArrayList<>();
			final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			for (int year = 2004; year <= 2018; year++) {
				for (int month = 1; month <= 12; month++) {
					try {
						timestamps.add(formatter.parse(String.format("%d%02d01", year, month)).getTime());
					} catch (java.text.ParseException e) {
						System.err.println("basdoawrd");
					}
					;
				}
			}

			System.out.println("Process in parallel");
			Optional<Map<Long, Integer>> totals = zoomIds.parallelStream().map(zoomId -> {
				try (final PreparedStatement pstmt = conn
						.prepareStatement("select data from grid_node where level = ? and id = ?")) {
					pstmt.setInt(1, zoomId.zoom);
					pstmt.setLong(2, zoomId.id);
					try (final ResultSet rst2 = pstmt.executeQuery()) {
						if (rst2.next()) {
							final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
							final GridOSHNodes hosmCell = (GridOSHNodes) ois.readObject();
							return hosmCell;
						} else {
							System.err.printf("ERROR: no result for level:%d and id:%d\n", zoomId.zoom, zoomId.id);
							return null;
						}
					}
				} catch (IOException | SQLException | ClassNotFoundException e) {
					e.printStackTrace();
					return null;
				}
			}).filter(filter -> filter != null).map(hosmCell -> {
				Map<Long, Integer> counts = new HashMap<>(timestamps.size());
				Iterator<OSHNode> oshNodeIt = hosmCell.iterator();
				while (oshNodeIt.hasNext()) {
					OSHNode oshNode = (OSHNode) oshNodeIt.next();
					Map<Long, OSMNode> osmNodes = oshNode.getByTimestamps(timestamps);
					for (Map.Entry<Long, OSMNode> node : osmNodes.entrySet()) {
						Long timestamp = node.getKey();
						OSMNode osmNode = node.getValue();
						if (osmNode.isVisible()) {
							Integer prevCnt = counts.get(timestamp);
							counts.put(timestamp, prevCnt != null ? prevCnt.intValue() + 1 : 0);
						}
					}
				}

				return counts;
			}).reduce((a, b) -> {
				Map<Long, Integer> sum = new TreeMap<>();
				Set<Long> ts = new HashSet<Long>();
				ts.addAll(a.keySet());
				ts.addAll(b.keySet());

				for (Long t : ts) {
					Integer aCnt = a.get(t);
					Integer bCnt = b.get(t);
					sum.put(t, (aCnt != null ? aCnt.intValue() : 0) + (bCnt != null ? bCnt.intValue() : 0));
				}
				return sum;
			});

			for (Map.Entry<Long, Integer> total : totals.get().entrySet()) {
				System.out.printf("%s\t%d\n", formatter.format(new Date(total.getKey())), total.getValue());
			}
		}
	}
}

