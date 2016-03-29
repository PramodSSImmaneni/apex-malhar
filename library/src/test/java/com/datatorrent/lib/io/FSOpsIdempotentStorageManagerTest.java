package com.datatorrent.lib.io;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 *
 */
public class FSOpsIdempotentStorageManagerTest {
    private static class TestMeta extends TestWatcher
    {
        String applicationPath;
        FSOpsIdempotentStorageManager storageManager;
        Context.OperatorContext context;

        @Override
        protected void starting(Description description)
        {
            super.starting(description);
            storageManager = new FSOpsIdempotentStorageManager();
            applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();

            Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
            attributes.put(DAG.APPLICATION_PATH, applicationPath);
            context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

            storageManager.setup(context);
        }

        @Override
        protected void finished(Description description)
        {
            storageManager.teardown();
            try {
                FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Rule
    public TestMeta testMeta = new TestMeta();

    @Test
    public void testOpIds() throws IOException
    {
        Map<Integer, String> data1 = Maps.newHashMap();
        data1.put(1, "one");
        data1.put(2, "two");
        data1.put(3, "three");

        Map<Integer, String> data2 = Maps.newHashMap();
        data2.put(4, "four");
        data2.put(5, "five");
        data2.put(6, "six");

        testMeta.storageManager.save(data1, 1, 1);
        testMeta.storageManager.save(data2, 3, 1);
        testMeta.storageManager.save(data2, 2, 1);
        testMeta.storageManager.save(data1, 1, 2);

        Set<Integer> opIds = testMeta.storageManager.getOperatorIds();
        Assert.assertTrue("Operator ids present", opIds.containsAll(Sets.newHashSet(1,2,3)));
    }

    @Test
    public void testOrderedWindowIds() throws IOException {
        Map<Integer, String> data1 = Maps.newHashMap();
        data1.put(1, "one");
        data1.put(2, "two");
        data1.put(3, "three");

        long[] windowIds = new long[10];

        for (int i = 1; i <= 10; ++i) {
            testMeta.storageManager.save(data1, 1, i);
            windowIds[i - 1] = i;
        }

        long[] owinIds = testMeta.storageManager.getOrderedWindowIds(1);
        Assert.assertArrayEquals("Window ids", windowIds, owinIds);
    }
}
