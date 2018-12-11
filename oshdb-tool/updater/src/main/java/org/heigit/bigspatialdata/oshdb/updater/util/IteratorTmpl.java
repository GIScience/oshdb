package org.heigit.bigspatialdata.oshdb.updater.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IteratorTmpl<T> implements Iterator<T> {
	protected T next;
	protected Exception ex;

	@Override
	public boolean hasNext() {
		try {
			return (next != null) || ((next = getNext()) != null);
		} catch (Exception e) {
			this.ex = e;
		}
		return false;
	}

	public T next() {
		if (!hasNext()) {
			if (ex != null)
				throw new RuntimeException(ex);
			throw new NoSuchElementException();
		}
		T ret = next;
		next = null;
		return ret;
	}

	public boolean hasException() {
		return ex != null;
	}

	public Exception getException() {
		return ex;
	}

	protected abstract T getNext() throws Exception;
}
