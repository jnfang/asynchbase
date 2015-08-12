/*
 * Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import com.stumbleupon.async.Callback;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertTrue;

import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import org.powermock.api.mockito.PowerMockito;

import static org.hbase.async.TestNSREs.mkregion;
import static org.hbase.async.TestNSREs.getStatic;
import static org.hbase.async.TestNSREs.anyBytes;
import static org.hbase.async.TestNSREs.metaRow;
import static org.hbase.async.TestNSREs.newDeferred;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, Scanner.class})
final class TestReverseScan {

  private static final byte[] COMMA = { ',' };
  private static final byte[] TIMESTAMP = "1234567890".getBytes();
  private static final byte[] INFO = getStatic("INFO");
  private static final byte[] REGIONINFO = getStatic("REGIONINFO");
  private static final byte[] SERVER = getStatic("SERVER");
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 'k', 'e', 'y' };
  private static final byte[] KEY2 = { 'b', 'r', 'o' };
  private static final byte[] FAMILY = { 'f' };
  private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
  private static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
  private static final RegionInfo region = mkregion("table", "table,,1234567890");
  private HBaseClient client = spy(new HBaseClient("test-quorum-spec"));
  /** Extracted from {@link #client}.  */
  private ConcurrentSkipListMap<byte[], RegionInfo> regions_cache;
  /** Extracted from {@link #client}.  */
  private ConcurrentHashMap<RegionInfo, RegionClient> region2client;
  /** Fake client supposedly connected to -ROOT-.  */
  private RegionClient rootclient = mock(RegionClient.class);
  /** Fake client supposedly connected to .META..  */
  private RegionClient metaclient = mock(RegionClient.class);
  /** Fake client supposedly connected to our fake test table.  */
  private RegionClient regionclient = mock(RegionClient.class);

  private long scanner_id = 1234;

 @Before
  public void before() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    // Inject a timer that always fires away immediately.
    //Whitebox.setInternalState(client, "timer", new FakeTimer());
    regions_cache = Whitebox.getInternalState(client, "regions_cache");
    region2client = Whitebox.getInternalState(client, "region2client");
    regions_cache.put(meta.name(), meta);
    region2client.put(meta, metaclient);
    regions_cache.put(region.name(), region);
    region2client.put(region, regionclient);
    // injectRegionInCache(meta, metaclient);
    // injectRegionInCache(region, regionclient);
  }

  @Test(expected=BrokenMetaException.class)
  public void simpleScan() throws Exception{
    String[] args = {"table", "f"};
    final Scanner spy_scanner = spy(client.newScanner(TABLE));
    spy_scanner.setStartKey(KEY);
    spy_scanner.setStopKey(KEY2);
    spy_scanner.setReverse();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);
    when (regionclient.isAlive()).thenReturn(true);
    when (metaclient.isAlive()).thenReturn(true);
    
    when (spy_scanner.isFirstReverseRegion()).thenReturn(false);
    when (client.getRegion(TABLE, KEY)).thenReturn(null);
    when (client.getRegion(TABLE, new byte[0])).thenReturn(region);

    byte[] meta_key = HBaseClient.createPreviousRegionSearchKey(TABLE, KEY);
    byte[] meta_name = HBaseClient.META;

    when (metaclient.getClosestRowBefore(eq(meta), eq(meta_name), eq(meta_key), eq(HBaseClient.INFO)))
    // .thenAnswer(newDeferred(metaRow()));
    .thenThrow(new BrokenMetaException(null, "Mock error from META lookup"));

    // when(client.locateRegionByLookup(TABLE, KEY, false))
    // .thenThrow(new BrokenMetaException(null, "Mock error from META"));

    final ArrayList<KeyValue> test_row = new ArrayList<KeyValue> ();
    // final KeyValue kv = new KeyValue(KEY);
    final KeyValue kv_host = new KeyValue(KEY, FAMILY, QUALIFIER,"localhost:54321".getBytes());
    test_row.add(kv_host);
    assertSame(row, spy_scanner.nextRows().joinUninterruptibly());

  }

  private static Scanner.Response makeScannerResponse(){
    final long scanner_id = 1234567;
    final ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>> ();
    final ArrayList<KeyValue> cols = new ArrayList<KeyValue>();
    final KeyValue val = KV;
    cols.add(val);
    rows.add(cols);
    final boolean more = false;

    return new Scanner.Response(scanner_id, rows, more);
  }  

}