package org.knime.rankaggregation;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.kanime.rankaggregation.factory.EDFDataTable;
import org.kanime.rankaggregation.factory.EDFFactory;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * @author Randy Reyna Hernandez
 * 
 *         This is the model implementation of EDFReader. This node reads in
 *         Election Data Format (EDF) from an URL. In the configuration dialog
 *         specify a valid URL and set an optional row prefix. A row ID is
 *         generated by the reader in the form 'prefix + rownumber'. If no
 *         prefix is specified, the row IDs are just the row numbers.
 *
 */
public class EDFReaderNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger LOGGER = NodeLogger.getLogger(EDFReaderNodeModel.class);

	static final String CFGKEY_FILEURL = "FileURL";
	static final String CFGKEY_ROWPREFIX = "rowPrefix";
	private URL m_url;

	private final SettingsModelString m_file = new SettingsModelString(EDFReaderNodeModel.CFGKEY_FILEURL, "");
	private final SettingsModelString m_rowPrefix = new SettingsModelString(EDFReaderNodeModel.CFGKEY_ROWPREFIX,
			"Voter");

	/**
	 * Constructor for the node model.
	 */
	protected EDFReaderNodeModel() {
		super(0, 1);
		reset();
	}

	/**
	 * Creates a new EDF reader with a default file.
	 *
	 * @param edfFileLocation
	 *            URL to the EDF file to read
	 */
	public EDFReaderNodeModel(final String edfFileLocation) {
		this();
		try {
			m_url = stringToURL(edfFileLocation);
		} catch (MalformedURLException mue) {
			LOGGER.error(mue.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
			throws Exception {
		assert m_url != null;
		if (m_url == null) {
			throw new NullPointerException("Please, configure EDFReader before executing it.");
		}

		EDFFactory table = new EDFFactory();
		EDFDataTable edfTable = table.createDataTable(m_url, m_rowPrefix.getStringValue(), exec);
		edfTable.llenarDataTable();
		Integer numberOfVoters = edfTable.getNumberOfVoters();
		pushFlowVariableInt("reader.voters", numberOfVoters);
		BufferedDataTable outTable = exec.createBufferedDataTable(edfTable, exec);
		return new BufferedDataTable[] { outTable };

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
	}

	/**
	 * No se puede conocer el formato de la tabla de salida hasta despues de ser
	 * ejecutado el nodo, pero no antes {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
		String warning = CheckUtils.checkSourceFile(m_url == null ? null : m_url.toString());
		if (warning != null) {
			setWarningMessage(warning);
		}
		pushFlowVariableString("reader.url", m_url.getPath());
		pushFlowVariableString("reader.prefix", m_rowPrefix.getStringValue());
		return new DataTableSpec[] { null };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {

		m_file.saveSettingsTo(settings);
		m_rowPrefix.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

		m_file.loadSettingsFrom(settings);
		try {
			m_url = stringToURL(m_file.getStringValue());
		} catch (MalformedURLException mue) {
			LOGGER.error(mue.getMessage());
		}
		m_rowPrefix.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {

		m_file.validateSettings(settings);
		m_rowPrefix.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
	}

	public static URL stringToURL(final String url) throws MalformedURLException {
		if ((url == null) || (url.equals(""))) {
			throw new MalformedURLException("URL not valid");
		}
		URL newURL;
		try {
			newURL = new URL(url);
		} catch (Exception e) {
			File tmp = new File(url);
			newURL = tmp.getAbsoluteFile().toURI().toURL();
		}
		return newURL;
	}
}
