package ptest.com.g414.st8.count;

import static com.sun.faban.driver.CycleType.THINKTIME;

import java.util.concurrent.TimeUnit;

import com.google.inject.Key;
import com.sun.faban.driver.BenchmarkDefinition;
import com.sun.faban.driver.BenchmarkDriver;
import com.sun.faban.driver.BenchmarkOperation;
import com.sun.faban.driver.FlatMix;
import com.sun.faban.driver.NegativeExponential;

@BenchmarkDefinition(name = "CounterBasicDriverScenario02", version = "1.0")
@BenchmarkDriver(name = "CounterBasicDriverScenario02", responseTimeUnit = TimeUnit.MICROSECONDS)
@FlatMix(operations = { "create", "update", "retrieve" }, mix = { 0.25, 0.25,
        0.50 })
public class CounterBasicDriverScenario02 extends
        CounterBasicDriverScenarioBase {
    public CounterBasicDriverScenario02() throws Exception {
        super();
    }

    @BenchmarkOperation(name = "create", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void create() throws Exception {
        super.create();
    }

    @BenchmarkOperation(name = "update", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void update() throws Exception {
        super.update();
    }

    @BenchmarkOperation(name = "retrieve", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void retrieve() throws Exception {
        super.retrieve();
    }

    public static class GuiceModule extends
            CounterBasicDriverScenarioBase.GuiceModule {
        @Override
        protected void configure() {
            super.configure();

            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    CounterBasicDriverScenario02.class);
        }
    }
}