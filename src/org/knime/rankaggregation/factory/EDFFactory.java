package org.knime.rankaggregation.factory;

import java.net.URL;

import org.knime.core.node.ExecutionContext;
import org.knime.rankaggregation.tables.AbstractEDFFile;
import org.knime.rankaggregation.tables.EDFMjg;
import org.knime.rankaggregation.tables.EDFPwg;
import org.knime.rankaggregation.tables.EDFSoc;
import org.knime.rankaggregation.tables.EDFSoi;
import org.knime.rankaggregation.tables.EDFToc;
import org.knime.rankaggregation.tables.EDFTog;
import org.knime.rankaggregation.tables.EDFToi;
import org.knime.rankaggregation.tables.EDFWmg;

public class EDFFactory extends AbstractEDFFactory{
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
}
