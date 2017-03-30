package org.heigit.bigspatialdata.oshpbf;

import java.util.ArrayList;
import java.util.List;

public class HeaderInfo {

	private final List<String> requiredFeatures = new ArrayList<String>();

	public List<String> getRequiredFeatures() {
		return requiredFeatures;
	}

	public List<String> getOptionalFeatures() {
		return optionalFeatures;
	}

	public String getWritingProgram() {
		return writingProgram;
	}

	public String getSource() {
		return source;
	}

	public long getReplicationTimestamp() {
		return replicationTimestamp;
	}

	public long getReplicationSequenceNumber() {
		return replicationSequenceNumber;
	}

	public String getReplicationBaseUrl() {
		return replicationBaseUrl;
	}

	private final List<String> optionalFeatures = new ArrayList<String>();

	private String writingProgram;
	private String source;
	private long replicationTimestamp;
	private long replicationSequenceNumber;
	private String replicationBaseUrl;

	public void addRequiredFeatures(String requiredFeature) {
		requiredFeatures.add(requiredFeature);

	}

	public void addOptionalFeatures(String optionalFeature) {
		optionalFeatures.add(optionalFeature);

	}

	public void setWrittingProgram(String writingprogram) {
		this.writingProgram = writingprogram;

	}

	public void setSource(String source) {
		this.source = source;

	}

	public void setReplicationTimestamp(long osmosisReplicationTimestamp) {
		this.replicationTimestamp = osmosisReplicationTimestamp;

	}

	public void setReplicationSequenceNumber(long osmosisReplicationSequenceNumber) {
		this.replicationSequenceNumber = osmosisReplicationSequenceNumber;

	}

	public void setReplicationBaseUrl(String osmosisReplicationBaseUrl) {
		this.replicationBaseUrl = osmosisReplicationBaseUrl;

	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("required: ");
		for (int i = 0; i < requiredFeatures.size(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(requiredFeatures.get(i));
		}
		sb.append("\n");
		sb.append("optional: ");
		for (int i = 0; i < optionalFeatures.size(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(optionalFeatures.get(i));
		}
		sb.append("\n");

		sb.append("writtingProgram: ").append(writingProgram).append("\n");
		sb.append("source: ").append(source).append("\n");
		sb.append("replication:").append("\n");
		sb.append("  Timestamp: ").append(replicationTimestamp).append("\n");
		sb.append("  SequenceNumber: ").append(replicationSequenceNumber).append("\n");
		sb.append("  BaseUrl: ").append(replicationBaseUrl);
		return sb.toString();
	}

}