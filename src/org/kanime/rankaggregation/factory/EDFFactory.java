package org.kanime.rankaggregation.factory;

<<<<<<< HEAD
import org.knime.rankaggregation.tables.EDFMjg;
import org.knime.rankaggregation.tables.EDFPwg;
import org.knime.rankaggregation.tables.EDFSoc;
import org.knime.rankaggregation.tables.EDFSoi;
import org.knime.rankaggregation.tables.EDFToc;
import org.knime.rankaggregation.tables.EDFTog;
import org.knime.rankaggregation.tables.EDFToi;
import org.knime.rankaggregation.tables.EDFWmg;

import java.net.URL;

import org.knime.core.node.ExecutionContext;
import org.knime.rankaggregation.tables.AbstractEDFFile;
=======
import java.net.URL;
import org.apache.commons.io.FilenameUtils;
import org.knime.core.node.ExecutionContext;
import org.knime.rankaggregation.tables.DataTableMjg;
import org.knime.rankaggregation.tables.DataTablePwg;
import org.knime.rankaggregation.tables.DataTableSoc;
import org.knime.rankaggregation.tables.DataTableSoi;
import org.knime.rankaggregation.tables.DataTableToc;
import org.knime.rankaggregation.tables.DataTableTog;
import org.knime.rankaggregation.tables.DataTableToi;
import org.knime.rankaggregation.tables.DataTableWmg;
>>>>>>> 1f1488e9c892bf62f0f95d5d7a5ced030afdaff3

/**
 * 
 * @author Randy Reyna Hernández
 * 
 */

<<<<<<< HEAD
public final class EDFFactory extends AbstractEDFFactory{
	
	AbstractEDFFile file;
	URL url;
	String rowPrefix;
	ExecutionContext exec;
	
	public EDFFactory(URL url, String rowPrefix, ExecutionContext exec) {
		super();
		this.url = url;
		this.rowPrefix = rowPrefix;
		this.exec = exec;
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

	public String getRowPrefix() {
		return rowPrefix;
	}

	public void setRowPrefix(String rowPrefix) {
		this.rowPrefix = rowPrefix;
	}

	public ExecutionContext getExec() {
		return exec;
	}

	public void setExec(ExecutionContext exec) {
		this.exec = exec;
	}

	@Override
	public AbstractEDFFile readData(String extension) {
		switch (extension) {
		case "mjg": {
			file =  new EDFMjg(url, rowPrefix, exec);
			break;
		}
		case "pwg": {
			file =  new EDFPwg(url, rowPrefix, exec);
			break;
		}
		case "soc": {
			file =  new EDFSoc(url, rowPrefix, exec);
			break;
		}
		case "soi": {
			file =  new EDFSoi(url, rowPrefix, exec);
			break;
		}
		case "toc": {
			file =  new EDFToc(url, rowPrefix, exec);
			break;
		}
		case "tog": {
			file =  new EDFTog(url, rowPrefix, exec);
			break;
		}
		case "toi": {
			file =  new EDFToi(url, rowPrefix, exec);
			break;
		}
		case "wmg": {
			file =  new EDFWmg(url, rowPrefix, exec);
			break;
		}
		default:
			file =  new EDFSoc(url, rowPrefix, exec);
			break;
		}
		return file;
	}
=======
public final class EDFFactory extends AbstractFactory {

	EDFDataTable EDFtable;
	public EDFFactory() {
	}

	@Override
	public EDFDataTable createDataTable(URL url, String prefix, ExecutionContext exec) throws Exception {
		String extension = getFileExtension(url);
		switch (extension) {
		case "mjg": {
			return new DataTableMjg(url, prefix, exec);
		}
		case "pwg": {
			return new DataTablePwg(url, prefix, exec);
		}
		case "soc": {
			return new DataTableSoc(url, prefix, exec);
		}
		case "soi": {
			return new DataTableSoi(url, prefix, exec);
		}
		case "toc": {
			return new DataTableToc(url, prefix, exec);
		}
		case "tog": {
			return new DataTableTog(url, prefix, exec);
		}
		case "toi": {
			return new DataTableToi(url, prefix, exec);
		}
		case "wmg": {
			return new DataTableWmg(url, prefix, exec);
		}
		default:
			return new DataTableToi(url, prefix, exec);
		}
	}

	private String getFileExtension(URL url) {
		String extension = FilenameUtils.getExtension(url.getPath());
		return extension;
	}

>>>>>>> 1f1488e9c892bf62f0f95d5d7a5ced030afdaff3
}
