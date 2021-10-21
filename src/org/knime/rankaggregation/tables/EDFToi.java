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
 *         Each file with a toi extension contains a profile consisting of an
 *         transitive relation where not all elements appear in every list.
 *         These written A,{B,C},D; meaning A is strictly preferred to {B,C},
 *         while the voter is indifferent between B and C, with A,B, and C all
 *         preferred to D. The strict relation is always denoted by use of a
 *         comma (,) while elements that an agent is indifferent between are
 *         grouped in a curly brace ({}).
 */

public final class EDFToi extends AbstractEDFFile {

	private final String TABLENAME = "Orders with Ties - Incomplete List";

	private int warningTiesList = 0;
	private int warningCompleteList = 0;

	public EDFToi(URL url, String rowPrefix, ExecutionContext exec) {
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
		config.addQuotePattern("{", "}", true);
		config.setCombineMultipleDelimiters(true);
		config.allowLFinQuotes(true);
		return config;
	}

	@Override
	public DataCell[] readDataFile() throws IllegalStateException {
		DataTableSpec table = getDataTableSpec();
		int noOfCols = table.getNumColumns();
		DataCell[] rowCells = new DataCell[noOfCols];
		String token = "";
		int ordenRank = 0;
		int column = 0;
		boolean withTies = false;
		String attributePosition;
		Integer tablePosition;
		while (column < noOfCols) {
			token = getTokenizer().nextToken();
			if (token == null || token.equals("\n")) {
				break;
			}
			token = token.trim();
			if (!token.isEmpty() && token.charAt(0) == '{') {
				if (token.contains("\n")) {
					throw new IllegalStateException(
							"Malformatted tie data, No symbol '}' found. In line: " + (getFileLineNumber() - 1));
				}
				if (token.charAt(token.length() - 1) != '}') {
					throw new IllegalStateException(
							"Malformatted tie data, No symbol '}' found. In line: " + getFileLineNumber());
				}
				withTies = true;
				token = token.substring(1, token.length() - 1);
				String[] empatados = token.split(",");
				int col;
				column += empatados.length;
				for (int i = 0; i < empatados.length; i++) {
					try {
						col = Integer.parseInt(empatados[i].trim());
						if (rowCells[col] == null) {
							rowCells[col] = createNewDataCellOfType(table.getColumnSpec(col).getType(),
									String.valueOf(ordenRank));
						} else {
							if (col == 0) {
								throw new IllegalStateException(
										"EDF reader ERROR: Wrong data format. All data values separated by ',' have be in the interval(1; "
												+ (noOfCols - 1) + "). In line: " + getFileLineNumber());
							} else {
								throw new IllegalStateException(
										"EDF reader ERROR: Wrong data format. An element was ordered twice in the list. In line  "
												+ getFileLineNumber());
							}
						}

					} catch (NumberFormatException e) {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. All data values separated by '{.,.}' have be integers. In line "
										+ getFileLineNumber() + " read '" + empatados[i].trim() + "' for an integer.");
					} catch (ArrayIndexOutOfBoundsException e) {
						throw new IllegalStateException(
								"EDF reader ERROR: Wrong data format. All values between '{.,.}' have be in the interval(1; "
										+ (noOfCols - 1) + "), error en la linea: " + getFileLineNumber());
					}
				}
			} else {
				try {
					if (ordenRank == 0) {
						attributePosition = token;
						tablePosition = 0;
					} else {
						attributePosition = String.valueOf(ordenRank);
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
			ordenRank++;
		}
		if (!withTies) {
			warningTiesList++;
			if (warningTiesList == getNumberUniqueOrders()) {
				throw new IllegalStateException("EDF reader ERROR: Order without ties file");
			}
		}
		if (ordenRank < noOfCols) {
			for (int i = 0; i < rowCells.length; i++) {
				if (rowCells[i] == null) {
					rowCells[i] = createNewDataCellOfType(table.getColumnSpec(i).getType(), "0");
				}
			}
		} else {
			warningCompleteList++;
			if (warningCompleteList == getNumberUniqueOrders()) {
				throw new IllegalStateException("EDF reader ERROR: Complete List File");
			}
		}
		readUntilEOL(token);
		return rowCells;
	}
}
