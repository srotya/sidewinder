/**
 * Copyright Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.cluster.pull.api;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.commons.codec.binary.Base64;

import com.srotya.sidewinder.cluster.Utils;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.ByteUtils;

public class MerkleTreeUtils {

//	public static void buildMerkleTreeForSeries(long startTime, long endTime, List<MerkleTree> trees,
//			SeriesFieldMap seriesFromKey, List<Tag> tags, String field)
//			throws NoSuchAlgorithmException, IOException {
//		List<MerkleNode> treeList = new ArrayList<>();
//		TimeSeries timeSeries = seriesFromKey.get(field);
//		SortedMap<Integer, List<Writer>> bucketRawMap = timeSeries.getBucketRawMap();
//		bucketRawMap = Utils.checkAndScopeTimeRange(startTime, endTime, timeSeries, bucketRawMap);
//		for (Entry<Integer, List<Writer>> entry : bucketRawMap.entrySet()) {
//			List<Writer> value = entry.getValue();
//			for (int i = 0; i < value.size(); i++) {
//				Writer writer = value.get(i);
//				if (writer.isReadOnly() || writer.isFull()) {
//					treeList.add(new MerkleNode(entry.getKey() + "_" + i + "_"
//							+ Base64.encodeBase64String(writer.getReader().getDataHash()), null, null));
//				}
//			}
//		}
//		MerkleNode root = computeMerkleNode(treeList);
//		MerkleTree tree = new MerkleTree();
//		tree.setRoot(root);
//		tree.setTags(tags);
//		tree.setValueFieldName(field);
//		trees.add(tree);
//	}

	public static MerkleNode computeMerkleNode(List<MerkleNode> tree) throws NoSuchAlgorithmException {
		if (tree.size() == 0) {
			return null;
		} else if (tree.size() == 1) {
			return tree.get(0);
		} else {
			int splitIndex = tree.size() / 2;
			List<MerkleNode> sa1 = tree.subList(0, splitIndex);
			MerkleNode left = computeMerkleNode(sa1);
			List<MerkleNode> sa2 = tree.subList(splitIndex, tree.size());
			MerkleNode right = computeMerkleNode(sa2);
			String value = (left != null ? left.getValue() : "") + "+" + (right != null ? right.getValue() : "");
			byte[] md5 = ByteUtils.md5(value.getBytes());
			return new MerkleNode(Base64.encodeBase64String(md5), left, right);
		}
	}

	public static class MerkleTree {

		private String valueFieldName;
		private List<Tag> tags;
		private MerkleNode root;

		/**
		 * @return the valueFieldName
		 */
		public String getValueFieldName() {
			return valueFieldName;
		}

		/**
		 * @param valueFieldName
		 *            the valueFieldName to set
		 */
		public void setValueFieldName(String valueFieldName) {
			this.valueFieldName = valueFieldName;
		}

		/**
		 * @return the tags
		 */
		public List<Tag> getTags() {
			return tags;
		}

		/**
		 * @param tags
		 *            the tags to set
		 */
		public void setTags(List<Tag> tags) {
			this.tags = tags;
		}

		/**
		 * @return the root
		 */
		public MerkleNode getRoot() {
			return root;
		}

		/**
		 * @param root
		 *            the root to set
		 */
		public void setRoot(MerkleNode root) {
			this.root = root;
		}

	}

	public static class MerkleNode {

		private String value;
		private MerkleNode leftChild;
		private MerkleNode rightChild;

		public MerkleNode(String value, MerkleNode leftChild, MerkleNode rightChild) {
			this.value = value;
			this.leftChild = leftChild;
			this.rightChild = rightChild;
		}

		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}

		/**
		 * @param value
		 *            the value to set
		 */
		public void setValue(String value) {
			this.value = value;
		}

		/**
		 * @return the leftChild
		 */
		public MerkleNode getLeftChild() {
			return leftChild;
		}

		/**
		 * @param leftChild
		 *            the leftChild to set
		 */
		public void setLeftChild(MerkleNode leftChild) {
			this.leftChild = leftChild;
		}

		/**
		 * @return the rightChild
		 */
		public MerkleNode getRightChild() {
			return rightChild;
		}

		/**
		 * @param rightChild
		 *            the rightChild to set
		 */
		public void setRightChild(MerkleNode rightChild) {
			this.rightChild = rightChild;
		}

	}

}
