package com.zkh.mr.tohbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义数据格式
 * partitioner 分区
 *
 */
public class ProductModel implements WritableComparable<ProductModel> {
	private String id;
	private String name;
	private String price;

	public ProductModel() {
		super();
	}

	public ProductModel(String id, String name, String price) {
		super();
		this.id = id;
		this.name = name;
		this.price = price;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.id);
		out.writeUTF(this.name);
		out.writeUTF(this.price);
	}

	
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.name = in.readUTF();
		this.price = in.readUTF();
	}


	public int compareTo(ProductModel o) {
		if (this == o) {
			return 0;
		}

		int tmp = this.id.compareTo(o.id);
		if (tmp != 0) {
			return tmp;
		}
		tmp = this.name.compareTo(o.name);
		if (tmp != 0) {
			return tmp;
		}
		tmp = this.price.compareTo(o.price);
		return tmp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((price == null) ? 0 : price.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductModel other = (ProductModel) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		return true;
	}
}
