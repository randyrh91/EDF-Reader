package org.knime.rankaggregation.tables;

import java.net.URL;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.util.tokenizer.TokenizerSettings;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 *         Files with a wmg extension describe a majority graph (relation) with
 *         weights. A majority graph is a directed graph over a set of
 *         alternatives. In our formatting we simply state the list of pairwise
 *         relations that described the majority graph (A,B; B,C) along with the
 *         number of times the particular relation was listed (A,B) minus the
 *         number of times the reverse relation was listed (B,A). The strict
 *         relation is always denoted by use of a comma (,). Note that majority
 *         graphs need not be complete, additionally there is no concept of a
 *         "tie" between two elements.
 */

public final class EDFWmg extends AbstractEDFFile {

	private final String TABLENAME = "Weighted Majority Graph";

	public EDFWmg(URL url, String rowPrefix, ExecutionContext exec) {
		super(url, rowPrefix, exec);
	}

	@Override
	public boolean isRightGraph() {
		return true;
	}

	@Override
	public boolean isGraph() {
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
		String attributePosition;
		Integer tablePosition;
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
					attributePosition = token;
					tablePosition = 0;
				} else {
					attributePosition = String.valueOf(column);
					tablePosition = Integer.parseInt(token);
				}
				if (rowCells[tablePosition] != null) {
					if (tablePosition == 0) {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. All data values separated by ',' have be in the interval(1; "
										+ (noOfCols - 1) + "). In line: " + getFileLineNumber());
					} else {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. An element was ordered twice in the list. In line  "
										+ getFileLineNumber());
					}
				} else {
					rowCells[tablePosition] = createNewDataCellOfType(table.getColumnSpec(tablePosition).getType(),
							attributePosition);
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
