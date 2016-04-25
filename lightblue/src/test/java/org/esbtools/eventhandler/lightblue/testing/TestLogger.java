/*
 *  Copyright 2016 esbtools Contributors and/or its affiliates.
 *
 *  This file is part of esbtools.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.esbtools.eventhandler.lightblue.testing;

import org.junit.AssumptionViolatedException;
import org.junit.rules.Stopwatch;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TestLogger extends TestWatcher {
    private static final Logger log = LoggerFactory.getLogger(TestLogger.class);

    @Override
    protected void starting(Description description) {
        log.info("Test starting: {}", description);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return super.apply(new Stopwatch() {
            @Override
            protected void succeeded(long nanos, Description description) {
                log.info("Test succeeded after {}: {}", Duration.ofNanos(nanos), description);
            }

            @Override
            protected void failed(long nanos, Throwable e, Description description) {
                log.info("Test failed after {}: {}", Duration.ofNanos(nanos), description);
            }

            @Override
            protected void skipped(long nanos, AssumptionViolatedException e, Description description) {
                log.info("Test skipped after {}: {}", Duration.ofNanos(nanos), description);
            }
        }.apply(base, description), description);
    }
}
