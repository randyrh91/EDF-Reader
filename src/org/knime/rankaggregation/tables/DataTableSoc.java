package org.knime.rankaggregation.tables;

import java.net.URL;
import org.kanime.rankaggregation.factory.EDFDataTable;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.util.tokenizer.TokenizerSettings;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 *         Each file with a soc extension contains a profile consisting of a
 *         complete, transitive, and asymmetric relation over a group of
 *         objects. These are written A,B,C; meaning that A is strictly
 *         preferred to B which is strictly preferred to C. The strict relation
 *         is always denoted by use of a comma (,).
 */

public final class DataTableSoc extends EDFDataTable {

	private final String TABLENAME = "Strict Orders - Complete List";

	public DataTableSoc(URL url, String rowPrefix, ExecutionContext exec) {
		super(url, rowPrefix, exec);
	}

	@Override
	public boolean esGrafoCorrecto() {
		return true;
	}

	@Override
	public boolean esGrafo() {
		return false;
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
	public DataCell[] readDataFile() throws IllegalStateException {

		DataTableSpec table = getDataTableSpec();
		int noOfCols = table.getNumColumns();
		DataCell[] rowCells = new DataCell[noOfCols];
		String token = "";
		int column = 0;
		String posicionAtributo;
		Integer posicionTable;
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
				} else {
					posicionAtributo = String.valueOf(column);
					posicionTable = Integer.parseInt(token);
				}
				if (rowCells[posicionTable] != null) {
					if (posicionTable == 0) {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. All data values separated by ',' have be in the interval(1; "
										+ (noOfCols - 1) + ") in line: " + getFileLineNumber());
					} else {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. An element was ordered twice in the list in line  "
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
								+ (noOfCols - 1) + ") in line: " + getFileLineNumber());
			}
			column++;
		}

		if (column < noOfCols) {
			throw new IllegalStateException(
					"EDF reader ERROR: Wrong data format. Incomplete List in line: " + getFileLineNumber());
		}
		readUntilEOL(token);
		return rowCells;
	}

}
