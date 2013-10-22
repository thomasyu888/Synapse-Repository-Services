package org.sagebionetworks.authutil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.openid4java.discovery.DiscoveryException;
import org.openid4java.discovery.DiscoveryInformation;
import org.openid4java.discovery.Identifier;
import org.openid4java.discovery.UrlIdentifier;
import org.sagebionetworks.repo.model.auth.DiscoveryInfo;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;

public class DiscoveryInfoUtils {

	@SuppressWarnings("unchecked")
	public static DiscoveryInfo convertObjectToDTO(DiscoveryInformation discInfo) {
		DiscoveryInfo dto = new DiscoveryInfo();
		dto.setOpenIdEndpoint(discInfo.getOPEndpoint().toString());
		
		Identifier identifier = discInfo.getClaimedIdentifier();
		if (identifier != null) {
			dto.setIdentifier(identifier.getIdentifier());
		}
		
		dto.setDelegate(discInfo.getDelegateIdentifier());
		dto.setVersion(discInfo.getVersion());
		dto.setServiceTypes((Set<String>) discInfo.getTypes());
		return dto;
	}
	
	public static DiscoveryInformation convertDTOToObject(DiscoveryInfo discInfo) throws MalformedURLException {
		URL endpoint = new URL(discInfo.getOpenIdEndpoint());
		try {
			Identifier identifier = null;
			if (discInfo.getIdentifier() != null) {
				identifier = new UrlIdentifier(discInfo.getIdentifier());
			}
			
			DiscoveryInformation obj = new DiscoveryInformation(endpoint, identifier, 
					discInfo.getDelegate(), 
					discInfo.getVersion(), 
					discInfo.getServiceTypes());
			return obj;
		} catch (DiscoveryException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String zipDTO(DiscoveryInfo discInfo) throws JSONObjectAdapterException, IOException {
		String dtoString = EntityFactory.createJSONStringForEntity(discInfo);
		
		// Zip up the bytes
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream zipped = new GZIPOutputStream(out);
		zipped.write(dtoString.getBytes());
		zipped.flush();
		zipped.close();
		
		// Base64 encode the bytes
		byte[] zippedAndBase64Encoded = Base64.encodeBase64(out.toByteArray());
		return new String(zippedAndBase64Encoded);
	}
	
	public static DiscoveryInfo unzipDTO(String zippedAndBase64Encoded) throws IOException, JSONObjectAdapterException {
		byte[] zipped = Base64.decodeBase64(zippedAndBase64Encoded.getBytes());
		
		ByteArrayInputStream in = new ByteArrayInputStream(zipped);
		GZIPInputStream unzip = new GZIPInputStream(in);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		while (unzip.available() > 0) {
			out.write(unzip.read());
		}
		
		String dtoString = new String(out.toByteArray());
		return EntityFactory.createEntityFromJSONString(dtoString, DiscoveryInfo.class);
	}
}