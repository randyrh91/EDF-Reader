package org.knime.rankaggregation.tables;

import java.net.URL;
import java.util.ArrayList;
import org.kanime.rankaggregation.factory.EDFDataTable;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.util.tokenizer.TokenizerSettings;

/**
 * 
 * @author Randy Reyna Hernández
 *
 *         Files with a tog extension describe a tournament graph. A tournament
 *         graph is a complete directed graph over a set of alternatives. In our
 *         formatting we simply state the list of pairwise relations that
 *         describe the tournament (A,B; B,C). The strict relation is always
 *         denoted by use of a comma (,). Note that tournaments must be complete
 *         assignment of the strict preference relation, otherwise it is a
 *         majority graph (see below).
 */

public final class DataTableTog extends EDFDataTable {

	private final String TABLENAME = "Tournament Graph";

	public DataTableTog(URL url, String rowPrefix, ExecutionContext exec) {
		super(url, rowPrefix, exec);
	}

	@Override
	public boolean esGrafoCorrecto() {
		return getGrafo().esTorneo();
	}

	@Override
	public boolean esGrafo() {
		return true;
	}

	@Override
	public String getTableName() {
		return this.TABLENAME;
	}

	@Override
	protected TokenizerSettings getTokenizerSettings() {
		TokenizerSettings config = new TokenizerSettings();
		config.addDelimiterPattern("\n", true, true, false);
		config.addDelimiterPattern(",", false, false, false);
		config.addDelimiterPattern("\t", true, false, false);
		config.setCombineMultipleDelimiters(true);
		return config;
	}

	@Override
	public DataCell[] readDataFile() throws IllegalStateException, InvalidSettingsException {
		DataTableSpec table = getDataTableSpec();
		int noOfCols = table.getNumColumns();
		DataCell[] rowCells = new DataCell[noOfCols];
		String token = "";
		int column = 0;
		String posicionAtributo;
		Integer posicionTable;
		ArrayList<Integer> dataList = new ArrayList<Integer>();
		while (column < noOfCols) {
			token = getTokenizer().nextToken();
			if (token == null || token.equals("\n")) {
				break;
			}
			token = token.trim();
			if (!token.isEmpty() && token.charAt(0) == '{') {
				throw new IllegalStateException(
						"EDF reader ERROR: Wrong data format. Orders with ties in line: " + getFileLineNumber());
			}
			try {
				if (column == 0) {
					posicionAtributo = token;
					posicionTable = 0;
					dataList.add(Integer.parseInt(posicionAtributo));
				} else {
					posicionAtributo = String.valueOf(column);
					posicionTable = Integer.parseInt(token);
					dataList.add(posicionTable);
				}
				if (rowCells[posicionTable] != null) {
					if (posicionTable == 0) {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. All data values separated by ',' have be in the interval(1; "
										+ (noOfCols - 1) + "). In line: " + getFileLineNumber());
					} else {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. An element was ordered twice in the list. In line  "
										+ getFileLineNumber());
					}
				} else {
					rowCells[posicionTable] = createNewDataCellOfType(table.getColumnSpec(posicionTable).getType(),
							posicionAtributo);
				}
			} catch (NumberFormatException nfe) {
				throw new IllegalStateException(
						"EDF reader ERROR: Wrong data format. All data values separated by '{.,.}' have be integers. In line "
								+ getFileLineNumber() + " read '" + token + "' for an integer.");
			} catch (ArrayIndexOutOfBoundsException obe) {
				throw new IllegalStateException(
						"EDF reader ERROR: Wrong data format. All data values separated by ',' have be in the interval(1; "
								+ (noOfCols - 1) + "). In line: " + getFileLineNumber());
			}
			column++;
		}
		if (column != 3) {
			throw new IllegalStateException("EDF reader ERROR: Wrong data format.  " + getFileLineNumber());
		}
		boolean fueInsertada = getGrafo().insertarArista(dataList.get(1), dataList.get(2), dataList.get(0));
		if (!fueInsertada) {
			getLOGGER().warn("EDF reader WARNING: Wrong data format. Repeated edge or node with curl in line "
					+ getFileLineNumber());
		}
		dataList.clear();
		if (column < noOfCols) {
			for (int i = 0; i < rowCells.length; i++) {
				if (rowCells[i] == null) {
					rowCells[i] = createNewDataCellOfType(table.getColumnSpec(i).getType(), "0");
				}
			}
		}
		readUntilEOL(token);
		return rowCells;
	}
}
