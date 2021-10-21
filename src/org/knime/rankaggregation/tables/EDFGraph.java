package org.knime.rankaggregation.tables;

import java.util.ArrayList;
import org.knime.core.node.InvalidSettingsException;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public final class EDFGraph {
	private int nodes;
	private int edges;
	ArrayList<String> labels;
	private int[][] matrix;

	protected EDFGraph(int nodes, ArrayList<String> labels) {
		this.nodes = nodes;
		this.labels = labels;
		this.edges = 0;
		this.matrix = new int[this.nodes][this.nodes];
	}

	public int[][] getMatrix() {
		return matrix;
	}

	public boolean insertEdge(int startNode, int endNode, int peso) throws InvalidSettingsException {
		if (startNode - 1 <= this.nodes && endNode - 1 <= this.nodes) {
			if (startNode != endNode && this.matrix[startNode - 1][endNode - 1] == 0) {
				this.matrix[startNode - 1][endNode - 1] = peso;
				edges++;
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public boolean deleteEdge(int startNode, int endNode) throws InvalidSettingsException {
		if (startNode - 1 <= this.nodes && endNode - 1 <= this.nodes) {
			this.matrix[startNode - 1][endNode - 1] = 0;
			edges--;
			return true;
		} else {
			return false;
		}
	}

	public boolean existsEdge(int startNode, int endNode) throws InvalidSettingsException {
		if (startNode - 1 <= this.nodes && endNode - 1 <= this.nodes) {
			if (this.matrix[startNode - 1][endNode - 1] > 0) {
				return true;
			} else {
				return false;
			}
		} else {
			throw new InvalidSettingsException("Start node or end node are not in the graph");
		}
	}

	public int getEdgeWeight(int startNode, int endNode) throws InvalidSettingsException {
		if (startNode - 1 <= this.nodes && endNode - 1 <= this.nodes) {
			return this.matrix[startNode - 1][endNode - 1];
		} else {
			throw new InvalidSettingsException("Start node or end node are not in the graph");
		}
	}

	public String getLabelByNode(int node) throws InvalidSettingsException {
		if (node > 0 && node <= labels.size()) {
			return labels.get(node - 1);
		} else {
			throw new InvalidSettingsException("Node are not in the graph");
		}

	}

	public boolean isTournament() {
		return this.edges == ((this.nodes) * (this.nodes - 1)) / 2;
	}

	public boolean isPairwise() {
		return this.edges <= ((this.nodes) * (this.nodes - 1));
	}

}
