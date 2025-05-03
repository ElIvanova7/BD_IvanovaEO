package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
/**
 * The "Hourly Tips" упражнение из the Flink training
 *
 * Задача упражнения — сначала вычислить общую сумму чаевых, собранных каждым водителем, по часам, а затем,
 * на основе этого потока, найти самую высокую сумму чаевых за каждый час.
 *
 * Параметры:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        // Читаем параметры из командной строки.
        ParameterTool params = ParameterTool.fromArgs(args);
        // Путь к входному файлу с данными о тарифах.  Если параметр не указан, используется значение по умолчанию.
        final String inputPath = params.get("input", ExerciseBase.pathToFareData);

        // Максимальная допустимая задержка события (в секундах).
        final int maxDelay = 60;
        // Коэффициент ускорения обработки данных (для имитации реального времени).
        final int speedFactor = 600;

        // Создаем среду выполнения Flink.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Устанавливаем временную характеристику как Event Time.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Устанавливаем уровень параллелизма.
        env.setParallelism(ExerciseBase.parallelism);

        // Создаем поток данных о тарифах из источника данных.
        DataStream<TaxiFare> fareStream = env.addSource(
                fareSourceOrTest(new TaxiFareSource(inputPath, maxDelay, speedFactor)));

        // Вычисляем максимальную сумму чаевых за каждый час для каждого водителя.
        DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fareStream
                .keyBy(fare -> fare.driverId) // Группируем по ID водителя
                .timeWindow(Time.hours(1)) // Создаем временные окна по 1 часу
                .aggregate(new SumTipsAggregate(), new MaxTipsWindowFunction()) // Суммируем чаевые в окне, затем находим максимум
                .timeWindowAll(Time.hours(1)) // Создаем глобальные окна по 1 часу
                .maxBy(2); // Находим максимальную сумму чаевых в глобальном окне

        // Выводим или тестируем результат.
        printOrTest(hourlyMaxTips);

        // Запускаем выполнение.
        env.execute("Hourly Tips (java)");
    }

    // Функция агрегации для суммирования чаевых.
    public static class SumTipsAggregate implements AggregateFunction<TaxiFare, Float, Float> {

        @Override
        public Float createAccumulator() {
            return 0.0f; // Инициализируем аккумулятор нулем.
        }

        @Override
        public Float add(TaxiFare value, Float accumulator) {
            return accumulator + value.tip; // Добавляем чаевые к аккумулятору.
        }

        @Override
        public Float getResult(Float accumulator) {
            return accumulator; // Возвращаем накопленную сумму чаевых.
        }

        @Override
        public Float merge(Float a, Float b) {
            return a + b; // Объединяем два аккумулятора.
        }
    }

    // Функция для нахождения максимальной суммы чаевых в окне.
    public static class MaxTipsWindowFunction extends ProcessWindowFunction<
            Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<Float> input, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            // Получаем сумму чаевых из входных данных.
            Float totalTips = input.iterator().next();
            // Выводим результат: конец окна, ID водителя, сумма чаевых.
            out.collect(new Tuple3<>(context.window().getEnd(), key, totalTips));
        }
    }

}

