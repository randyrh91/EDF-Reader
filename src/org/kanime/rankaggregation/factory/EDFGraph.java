package org.kanime.rankaggregation.factory;

import java.util.ArrayList;
import org.knime.core.node.InvalidSettingsException;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public final class EDFGraph {
	private int cantNodos;
	private int cantAristas;
	ArrayList<String> labels;
	private int[][] matrizAdyacencia;

	protected EDFGraph(int cantidadNodos, ArrayList<String> labels) {
		this.cantNodos = cantidadNodos;
		this.labels = labels;
		this.cantAristas = 0;
		this.matrizAdyacencia = new int[this.cantNodos][this.cantNodos];
	}

	public int[][] getMatrizAdyacencia() {
		return matrizAdyacencia;
	}

	public boolean insertarArista(int nodoInicio, int NodoFin, int peso) throws InvalidSettingsException {
		if (nodoInicio - 1 <= this.cantNodos && NodoFin - 1 <= this.cantNodos) {
			if (nodoInicio != NodoFin && this.matrizAdyacencia[nodoInicio - 1][NodoFin - 1] == 0) {
				this.matrizAdyacencia[nodoInicio - 1][NodoFin - 1] = peso;
				cantAristas++;
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public boolean eliminarArista(int nodoInicio, int NodoFin) throws InvalidSettingsException {
		if (nodoInicio - 1 <= this.cantNodos && NodoFin - 1 <= this.cantNodos) {
			this.matrizAdyacencia[nodoInicio - 1][NodoFin - 1] = 0;
			cantAristas--;
			return true;
		} else {
			return false;
		}
	}

	public boolean existeArista(int nodoInicio, int NodoFin) throws InvalidSettingsException {
		if (nodoInicio - 1 <= this.cantNodos && NodoFin - 1 <= this.cantNodos) {
			if (this.matrizAdyacencia[nodoInicio - 1][NodoFin - 1] > 0) {
				return true;
			} else {
				return false;
			}
		} else {
			throw new InvalidSettingsException("Nodo inicio o nodo fin no estan en el grafo");
		}
	}

	public int getPesoArista(int nodoInicio, int NodoFin) throws InvalidSettingsException {
		if (nodoInicio - 1 <= this.cantNodos && NodoFin - 1 <= this.cantNodos) {
			return this.matrizAdyacencia[nodoInicio - 1][NodoFin - 1];
		} else {
			throw new InvalidSettingsException("Nodo inicio o nodo fin no estan en el grafo");
		}
	}

	public String getLabelByNode(int node) throws InvalidSettingsException {
		if (node > 0 && node <= labels.size()) {
			return labels.get(node - 1);
		} else {
			throw new InvalidSettingsException("El nodo no se encuentra en el grafo");
		}

	}

	public boolean esTorneo() {
		int totalAristas = ((this.cantNodos) * (this.cantNodos - 1)) / 2;
		return this.cantAristas == totalAristas;
	}

	public boolean esRelacionPares() {
		int totalAristas = ((this.cantNodos) * (this.cantNodos - 1));
		return this.cantAristas <= totalAristas;
	}

}
