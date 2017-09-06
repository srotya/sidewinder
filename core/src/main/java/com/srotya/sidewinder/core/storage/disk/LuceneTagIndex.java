/**
 * Copyright 2017 Ambud Sharma
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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.QueryBuilder;

import com.srotya.sidewinder.core.storage.TagIndex;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Lucene based tag index
 * 
 * @author ambud
 */
public class LuceneTagIndex implements TagIndex {

	private static final String ID = "id";
	private static final String TAG = "tag";
	private static final String ROW_KEY = "rowKey";
	private static final String TAG_2 = "tag2";
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private IndexWriter indexWriter;
	private XXHash32 hash;
	private StandardAnalyzer analyzer;
	private Directory index;

	public LuceneTagIndex(String indexDirectory, String measurementName) throws IOException {
		analyzer = new StandardAnalyzer();
//		index = new RAMDirectory();
		index = new MMapDirectory(new File(indexDirectory + "/" + measurementName).toPath());
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		config.setCommitOnClose(true);
		config.setMaxBufferedDocs(10000);
		config.setRAMBufferSizeMB(512);
		config.setRAMPerThreadHardLimitMB(256);
		config.setReaderPooling(true);
		indexWriter = new IndexWriter(index, config);
		hash = factory.hash32();
	}

	@Override
	public String createEntry(String tag) throws IOException {
		String hash32 = Integer.toHexString(hash.hash(tag.getBytes(), 0, tag.length(), 57));
		addDoc(indexWriter, tag, hash32);
		return hash32;
	}

	private static void addDoc(IndexWriter w, String tag, String tagHash) throws IOException {
		Document doc = new Document();
		doc.add(new TextField(TAG, tag, Field.Store.YES));
		doc.add(new TextField(ID, tagHash, Field.Store.NO));
		w.addDocument(doc);
	}

	@Override
	public String getEntry(String hexString) throws IOException {
		DirectoryReader reader = DirectoryReader.open(indexWriter);
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = new QueryBuilder(analyzer).createPhraseQuery(ID, hexString);
		TopDocs search = searcher.search(query, 1);
		return searcher.doc(search.scoreDocs[0].doc).get(TAG);
	}

	@Override
	public Set<String> getTags() throws IOException {
		DirectoryReader reader = DirectoryReader.open(indexWriter);
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = new QueryBuilder(analyzer).createPhraseQuery(TAG, "*");
		TopDocs search = searcher.search(query, Integer.MAX_VALUE);
		Set<String> set = new HashSet<>();
		Set<String> field = new HashSet<>(Arrays.asList(TAG));
		for (ScoreDoc doc : search.scoreDocs) {
			set.add(searcher.doc(doc.doc, field).get(TAG));
		}
		return set;
	}

	@Override
	public void index(String tag, String rowKey) throws IOException {
		Document doc = new Document();
		doc.add(new TextField(TAG_2, tag, Field.Store.NO));
		doc.add(new TextField(ROW_KEY, rowKey, Field.Store.YES));
		indexWriter.addDocument(doc);
	}

	@Override
	public Set<String> searchRowKeysForTag(String tag) throws IOException {
		DirectoryReader reader = DirectoryReader.open(indexWriter);
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = new QueryBuilder(analyzer).createPhraseQuery(TAG_2, tag);
		TopDocs search = searcher.search(query, Integer.MAX_VALUE);
		Set<String> set = new HashSet<>();
		Set<String> field = new HashSet<>(Arrays.asList(ROW_KEY));
		for (ScoreDoc doc : search.scoreDocs) {
			set.add(searcher.doc(doc.doc, field).get(ROW_KEY));
		}
		return set;
	}

	@Override
	public void close() throws IOException {
		indexWriter.commit();
		indexWriter.close();
		index.close();
	}

}
