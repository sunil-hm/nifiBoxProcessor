/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.box.processors;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
//import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.CreateUserParams;
import com.box.sdk.IAccessTokenCache;
import com.box.sdk.InMemoryLRUAccessTokenCache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Tags({ "files", "filesystem", "ingest", "ingress", "get", "source", "input", "box" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GetFileFromBox extends AbstractProcessor {

	public static final PropertyDescriptor BOX_CONFIG_FILE_PATH = new PropertyDescriptor.Builder()
			.name("BOX_CONFIG_FILE_PATH").displayName("Path to Box config file").description("Box config")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor BOX_USER_NAME = new PropertyDescriptor.Builder().name("BOX_USER_NAME")
			.displayName("Box user name").description("Box user through which account should be accessed")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor BOX_DIRECTORY = new PropertyDescriptor.Builder().name("BOX_DIRECTORY")
			.displayName("Box input directory").description("Box directory from which files should be accessed")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder().name("KEEP_SOURCE_FILE")
			.displayName("Keep source file").description("Flag indicates if the file needs to be kept after reading")
			.required(true).allowableValues("true", "false").defaultValue("true")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All files are routed to success").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	private ComponentLog logger = null;
	BoxFolder inputFolder = null;
	boolean keepSource = true;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		logger = getLogger();

		logger.info("initialized GetFileFromBox");
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(BOX_CONFIG_FILE_PATH);
		descriptors.add(BOX_USER_NAME);
		descriptors.add(BOX_DIRECTORY);
		descriptors.add(KEEP_SOURCE_FILE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	private BoxConfig getBoxConfig(String configFilePath) {
		BoxConfig boxConfig = null;
		try {
			Reader reader = new FileReader(configFilePath);
			boxConfig = BoxConfig.readFrom(reader);
			logger.info("Box config loaded succesfully");
		} catch (IOException ioe) {
			logger.error("Error in loading config file " + ioe);
			ioe.printStackTrace();
		}
		return boxConfig;
	}

	private BoxUser.Info getAppUser(String configPath, String userName, BoxConfig boxConfig) throws IOException {
		IAccessTokenCache accessTokenCache = new InMemoryLRUAccessTokenCache(10);

		BoxUser.Info boxUser = null;

		BoxDeveloperEditionAPIConnection api = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig,
				accessTokenCache);

		Iterator<BoxUser.Info> userIt = BoxUser.getAllEnterpriseUsers(api).iterator();
		while (userIt.hasNext()) {
			BoxUser.Info user = (BoxUser.Info) userIt.next();
			logger.info("box user " + user.getID() + " " + user.getName());

			if (user.getName().equals(userName)) {
				if (boxUser != null)
					logger.warn(userName + " box user encountered again!");
				boxUser = user;
			}
		}
		logger.info("matching box user " + boxUser.getID() + " " + boxUser.getName());
		return boxUser;
	}

	/**
	 * Gets the rootFolder of BoxAccount
	 * 
	 * @param configFile
	 * @param userId
	 * @return
	 * @throws IOException
	 */
	private BoxFolder getRootFolder(String configFile, String userId, BoxConfig boxConfig) throws IOException {

		IAccessTokenCache accessTokenCache = new InMemoryLRUAccessTokenCache(10);

		BoxDeveloperEditionAPIConnection api = BoxDeveloperEditionAPIConnection.getAppUserConnection(userId, boxConfig,
				accessTokenCache);

		return BoxFolder.getRootFolder(api);
	}

	/**
	 * Locates the folder to read the files from
	 * 
	 * @param currentBoxFolder
	 * @param path
	 * @return
	 */
	private BoxFolder locateInputFolder(BoxFolder currentBoxFolder, String path) {
		String currentFolder = null;
		int firstSeperatorAt = path.indexOf("/");
		if (firstSeperatorAt > 0)
			currentFolder = path.substring(0, firstSeperatorAt);
		else
			currentFolder = path;

		for (BoxItem.Info itemInfo : currentBoxFolder) {
			logger.info("itemInfo " + itemInfo.getName());
			if (itemInfo instanceof BoxFolder.Info) {
				BoxFolder childFolder = (BoxFolder) itemInfo.getResource();
				// folder name matches
				if (childFolder.getInfo().getName().equals(currentFolder)) {
					if (currentFolder.equals(path))// no more path left
						return childFolder;
					else
						return locateInputFolder(childFolder, path.substring(firstSeperatorAt + 1));// need
																									// to
																									// go
																									// deeper
				}
			}
		}
		// could not find the folder configured
		return null;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		logger.info("onScheduled");
		final String configFilePath = context.getProperty(BOX_CONFIG_FILE_PATH).getValue();
		final String boxUserName = context.getProperty(BOX_USER_NAME).getValue();
		final String inputDirectory = context.getProperty(BOX_DIRECTORY).getValue();
		keepSource = context.getProperty(KEEP_SOURCE_FILE).asBoolean().booleanValue();

		try {

			final BoxConfig boxConfig = getBoxConfig(configFilePath);

			if (boxConfig == null)
				throw new Exception("Could not create boxConfig");
			else
				logger.info("boxConfig " + boxConfig.getClientId());

			final BoxUser.Info boxUser = getAppUser(configFilePath, boxUserName, boxConfig);

			if (boxUser == null)
				throw new Exception("Could not locate boxUser");
			else
				logger.info("boxUser " + boxUser.getID());

			final BoxFolder rootFolder = getRootFolder(configFilePath, boxUser.getID(), boxConfig);

			if (rootFolder == null)
				throw new Exception("Could not retreive rootFolder ");
			else
				logger.info("rootfolder " + rootFolder.getInfo().getName());

			inputFolder = locateInputFolder(rootFolder, inputDirectory);

			if (inputFolder == null)
				throw new Exception("Could not locate inputFolder correponding to " + inputDirectory);
			else
				logger.info("inputFolder " + inputFolder.getInfo().getName());

		} catch (IOException ioe) {
			logger.error("IO Error in box interaction " + ioe);
			ioe.printStackTrace();
		} catch (Exception ex) {
			logger.error("Error in box interaction " + ex);
			ex.printStackTrace();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		for (BoxItem.Info dataItem : (BoxFolder) inputFolder) {
			if (dataItem instanceof BoxFile.Info) {
				BoxFile dataFile = (BoxFile) dataItem.getResource();
				ByteArrayOutputStream bop = new ByteArrayOutputStream();
				logger.debug("dataFile " + dataFile.getInfo().getName());

				FlowFile flowFile = session.create();
				dataFile.download(bop);

				flowFile = session.importFrom(new ByteArrayInputStream(bop.toByteArray()), flowFile);

				flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), dataFile.getInfo().getName());
				session.transfer(flowFile, REL_SUCCESS);
				if (!keepSource)
					dataFile.delete();
			}
		}
	}

	public static void main(String[] args) {
		try {
			BoxUser.Info boxUser = new GetFileFromBox()
					.getAppUser("/Users/sunilithappiri/Documents/Conns/box_key_config.json", "sunil_app_user", null);
			System.out.println("box user selected " + boxUser.getID());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}