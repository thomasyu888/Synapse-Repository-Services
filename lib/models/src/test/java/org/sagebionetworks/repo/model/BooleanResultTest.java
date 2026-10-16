package org.sagebionetworks.repo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.sagebionetworks.schema.ObjectSchema;
import org.sagebionetworks.schema.ObjectSchemaImpl;
import org.sagebionetworks.schema.TYPE;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;
import org.sagebionetworks.schema.adapter.org.json.JSONObjectAdapterImpl;

public class BooleanResultTest {

	@Test
	public void testWriteToJSONObjectWithTrue() throws JSONObjectAdapterException {
		// call under test
		String json = EntityFactory.createJSONStringForEntity(new BooleanResult(true));

		assertEquals(new JSONObject("{\"result\":true}").toMap(), new JSONObject(json).toMap());
	}

	@Test
	public void testRoundTripWithTrueAndFalse() throws JSONObjectAdapterException {
		for (boolean value : new boolean[] { true, false }) {
			String json = EntityFactory.createJSONStringForEntity(new BooleanResult(value));
			// call under test
			BooleanResult clone = EntityFactory.createEntityFromJSONString(json, BooleanResult.class);
			assertEquals(new BooleanResult(value), clone);
		}
	}

	@Test
	public void testInitializeFromJSONObjectWithMissingResult() throws JSONObjectAdapterException {
		// call under test
		BooleanResult result = EntityFactory.createEntityFromJSONString("{}", BooleanResult.class);

		assertEquals(new BooleanResult(false), result);
	}

	@Test
	public void testGetEffectiveSchema() throws JSONObjectAdapterException {
		// call under test
		ObjectSchema schema = new ObjectSchemaImpl(new JSONObjectAdapterImpl(new BooleanResult().getEffectiveSchema()));

		assertEquals(BooleanResult.class.getName(), schema.getId());
		assertEquals(TYPE.OBJECT, schema.getType());
		assertEquals(TYPE.BOOLEAN, schema.getProperties().get("result").getType());
	}
}
