package org.kanime.rankaggregation.factory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Vector;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.core.util.tokenizer.Tokenizer;
import org.knime.core.util.tokenizer.TokenizerSettings;

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

public abstract class EDFDataTable extends RowIterator implements DataTable {

	private final NodeLogger LOGGER = NodeLogger.getLogger(EDFDataTable.class);
	private DataTableSpec table;
	private EDFGraph grafo;
	private URL url;
	private String rowPrefix;
	private ExecutionContext exec;
	private Tokenizer tokenizer;
	private int rowNo;
	private int numberOfVoters;
	private int numberUniqueOrders;
	private int numMsgWrongFormat;
	private final int MAX_ERR_MSG = 10;
	private final String DEFAUL_PREFIX = "Voter";

	public EDFDataTable(URL url, String rowPrefix, ExecutionContext exec) {
		if (url == null) {
			throw new NullPointerException("Can't pass null EDF file location.");
		}
		this.url = url;
		this.rowPrefix = rowPrefix;
		this.exec = exec;
		this.rowNo = 1;
		this.numMsgWrongFormat = 0;
		if (rowPrefix == null) {
			this.rowPrefix = DEFAUL_PREFIX;
		} else {
			this.rowPrefix = rowPrefix;
		}
	}

	protected NodeLogger getLOGGER() {
		return LOGGER;
	}

	protected abstract TokenizerSettings getTokenizerSettings();

	protected Tokenizer getTokenizer() {
		return this.tokenizer;
	}

	protected void setTokenizer(Tokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}

	protected URL getUrl() {
		return url;
	}

	protected void setUrl(URL url) {
		this.url = url;
	}

	public int getNumberOfVoters() {
		return numberOfVoters;
	}

	protected int getNumberUniqueOrders() {
		return numberUniqueOrders;
	}

	protected int getRowNo() {
		return rowNo;
	}

	protected void setRowNo(int rowNo) {
		this.rowNo = rowNo;
	}

	protected DataTableSpec getTable() {
		return table;
	}

	protected EDFGraph getGrafo() {
		return grafo;
	}

	/**
	 * 
	 * @return DataCell[]
	 * @throws InvalidSettingsException
	 * @throws Exception
	 */
	protected abstract DataCell[] readDataFile() throws InvalidSettingsException;

	/**
	 * 
	 * @return String
	 */
	protected abstract String getTableName();

	/**
	 * 
	 * @return boolean
	 */
	protected abstract boolean esGrafoCorrecto();

	/**
	 * 
	 * @return boolean
	 */
	protected abstract boolean esGrafo();

	@Override
	public DataTableSpec getDataTableSpec() {
		return this.table;
	};

	@Override
	public RowIterator iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		String token = null;
		try {
			token = this.tokenizer.nextToken();
			while ((token != null) && (token.equals("\n") || (!this.tokenizer.lastTokenWasQuoted() && token.isEmpty()))
					|| token.trim().equals("")) {
				token = this.tokenizer.nextToken();
			}
			this.tokenizer.pushBack();
		} catch (Throwable t) {
			token = null;
		}
		if (!(token != null && this.rowNo <= this.numberUniqueOrders)) {
			if (this.rowNo <= this.numberUniqueOrders) {
				LOGGER.warn("EDF reader Warning: Incomplete data in file");
			}
			if (!esGrafoCorrecto()) {
				throw new IllegalStateException(
						"EDF reader ERROR: Wrong data format. File not describe a " + getTableName());
			}
			if (token != null) {
				LOGGER.warn("EDF reader Warning: Extra data in file");
			}
		}
		return (token != null && this.rowNo <= this.numberUniqueOrders);
	}

	@Override
	public DataRow next() {
		if (!hasNext()) {
			throw new NoSuchElementException("The row iterator proceeded beyond the last line.");
		}
		String rowID = this.rowPrefix + "_" + (this.rowNo - 1);
		DataCell[] rowCells;
		try {
			rowCells = this.readDataFile();
			rowNo++;
			return new DefaultRow(rowID, rowCells);
		} catch (InvalidSettingsException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected void readUntilEOL(String token) {
		boolean mensaje = false;
		while ((token != null) && !token.equals("\n")) {
			token = this.tokenizer.nextToken();
			if ((token != null) && !token.equals("\n") && !mensaje) {
				mensaje = true;
				if (numMsgWrongFormat < MAX_ERR_MSG) {
					LOGGER.warn("EDF Reader Warning:Ignoring extra data in line: " + getFileLineNumber());
					numMsgWrongFormat++;
				}
				if (numMsgWrongFormat == MAX_ERR_MSG) {
					LOGGER.warn(" (last message of this kind.)");
					numMsgWrongFormat++;
				}
			}
		}
	}

	protected int getFileLineNumber() {
		boolean lasted = getTokenizer().lastTokenWasDelimited();
		int lineNumber = (lasted) ? getTokenizer().getLineNumber() : getTokenizer().getLineNumber() - 1;
		return lineNumber;
	}

	private void createTokenizer() {
		InputStream inStream;
		try {
			inStream = FileUtil.openStreamWithTimeout(url);
			Tokenizer tokenizer = new Tokenizer(new BufferedReader(new InputStreamReader(inStream,  "UTF-8")));
			tokenizer.setSettings(this.getTokenizerSettings());
			this.tokenizer = tokenizer;
		} catch (IOException e) {
			throw new IllegalStateException("Can't open the file on the given URL");
		}
	}

	private void readHeaderDataFile() {
		String NumberOfUniqueOrders = "";
		try {
			String NumberOfVoters = this.tokenizer.nextToken().trim();
			this.numberOfVoters = Integer.parseInt(NumberOfVoters);
			String SumOfVoteCount = this.tokenizer.nextToken().trim();
			Integer.parseInt(SumOfVoteCount);
			NumberOfUniqueOrders = this.tokenizer.nextToken().trim();
			this.numberUniqueOrders = Integer.parseInt(NumberOfUniqueOrders);
		} catch (NumberFormatException nfe) {
			throw new IllegalStateException("EDF reader ERROR: Wrong data format. In line " + getFileLineNumber()
					+ " read data for tree integers.");
		}
		readUntilEOL(NumberOfUniqueOrders);
	}

	private void readHeaderFile() throws CanceledExecutionException, InvalidSettingsException {
		HashMap<Integer, String> atributos = new HashMap<Integer, String>();
		Vector<DataColumnSpec> colSpecs = new Vector<DataColumnSpec>();
		DataType type = IntCell.TYPE;
		boolean isEnd = false;
		while (!isEnd) {
			if (this.exec != null) {
				this.exec.checkCanceled(); // throws exception if user canceled.
			}
			String token = this.tokenizer.nextToken();
			if (token == null) {
				throw new InvalidSettingsException("Incorrect/Incomplete EDF file. No data found.");
			}
			token = token.trim();
			if (token.length() == 0) {
				continue;
			}
			int NumberOfCandidates;
			try {
				NumberOfCandidates = Integer.parseInt(token);
			} catch (NumberFormatException nfe) {
				throw new InvalidSettingsException("EDF reader ERROR: Wrong data " + "format. In line "
						+ getFileLineNumber() + " read '" + token.trim() + "' for an integer.");
			}
			this.readUntilEOL(this.tokenizer.nextToken());
			for (int i = 1; i <= NumberOfCandidates; i++) {
				token = this.tokenizer.nextToken();
				if (this.exec != null) {
					this.exec.checkCanceled();
				}
				if (token == null) {
					throw new InvalidSettingsException(
							"Incorrect/Incomplete EDF file. No data 'Number of Candidates' found. In line "
									+ getFileLineNumber());
				}
				token = token.trim();
				if (token.length() == 0) {
					i--;
					continue;
				}
				Integer key;
				try {
					key = Integer.parseInt(token.trim());
				} catch (NumberFormatException nfe) {
					throw new InvalidSettingsException("EDF reader ERROR: Wrong data format. In line "
							+ getFileLineNumber() + " read '" + token.trim() + "' for an integer.");
				}
				if (key != i) {
					if (i == 1) {
						throw new InvalidSettingsException(
								"EDF reader ERROR: Wrong data format. The keys of 'Candidate Name' have to begin with one. In line "
										+ getFileLineNumber());
					} else {
						throw new InvalidSettingsException(
								"EDF reader ERROR: Wrong data format. The keys of 'Candidate Name' have be serial numbers. In line "
										+ getFileLineNumber());
					}
				}

				token = this.tokenizer.nextToken();
				if (token == null) {
					throw new InvalidSettingsException(
							"Incorrect/Incomplete EDF file. No data 'Candidate Name' found.");
				}
				String CandidateName = token.trim();
				readUntilEOL(token);
				if (CandidateName.compareTo("") == 0) {
					throw new InvalidSettingsException(
							"The 'Candidate Name' is not defined. In line: " + getFileLineNumber());
				}

				if (!atributos.containsValue(CandidateName)) {
					atributos.put(key, CandidateName);
				} else
					throw new InvalidSettingsException(
							"Two equal 'Candidate Name' is defined. In line: " + getFileLineNumber());
				if (i == 1) {
					DataColumnSpecCreator dcscCount = new DataColumnSpecCreator("Count", type);
					colSpecs.add(dcscCount.createSpec());
				}

				DataColumnSpecCreator dcsc = new DataColumnSpecCreator(CandidateName, type);
				colSpecs.add(dcsc.createSpec());
			}
			this.table = new DataTableSpec(this.getTableName(), colSpecs.toArray(new DataColumnSpec[colSpecs.size()]));
			isEnd = true;
		}
	}

	protected DataCell createNewDataCellOfType(final DataType type, final String data) {
		if (type.equals(IntCell.TYPE)) {
			try {
				int val = Integer.parseInt(data.trim());
				return new IntCell(val);
			} catch (NumberFormatException nfe) {
				throw new IllegalStateException("EDF reader WARNING: Wrong data format. In line " + getFileLineNumber()
						+ " read '" + data + "' for an integer.");
			}
		} else {
			throw new IllegalStateException("Cannot create DataCell of type " + type.toString());
		}
	}

	private void createGraph() {
		ArrayList<String> labels = new ArrayList<String>();
		int numeroColumns = this.table.getNumColumns() - 1;
		for (int i = 0; i < numeroColumns; i++) {
			String label = getDataTableSpec().getColumnSpec(i).getName();
			labels.add(label);
		}
		this.grafo = new EDFGraph(numeroColumns, labels);
	}

	public void llenarDataTable() throws Exception {
		this.createTokenizer();
		this.readHeaderFile();
		if (this.esGrafo())
			this.createGraph();
		this.readHeaderDataFile();
	}
}
