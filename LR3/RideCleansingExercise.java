package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The "Ride Cleansing" упражнение из the Flink training
 Задача упражнения — отфильтровать поток данных о поездках такси, оставив только те поездки, которые
 * начинаются и заканчиваются в пределах Нью-Йорка. Результирующий поток должен быть выведен на печать.
 *
 * Параметры:
 *   -input path-to-input-file
 *
 */
public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        // Получаем параметры из командной строки.  "input" - путь к входному файлу, по умолчанию используется ExerciseBase.pathToRideData
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        // Максимальная задержка события (в секундах).  События могут быть не по порядку.
        final int maxEventDelay = 60;
        // Коэффициент ускорения обработки данных (для имитации реального времени).  Обрабатывается 10 минут данных за 1 секунду.
        final int servingSpeedFactor = 600;

        // Создаем среду выполнения Flink.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Устанавливаем уровень параллелизма.
        env.setParallelism(ExerciseBase.parallelism);

        // Создаем поток данных о поездках из источника данных.
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

        // Фильтруем поток, оставляя только поездки, которые начинаются и заканчиваются в Нью-Йорке.
        DataStream<TaxiRide> filteredRides = rides
                .filter(new NYCFilter()); // Применяем фильтр NYCFilter

        // Выводим или тестируем отфильтрованный поток.
        printOrTest(filteredRides);

        // Запускаем выполнение пайплайна.
        env.execute("Taxi Ride Cleansing");
    }

    // Внутренний класс для фильтрации поездок по географическому местоположению.
    private static class NYCFilter implements FilterFunction<TaxiRide> {

        // Минимальные и максимальные координаты Нью-Йорка (широта и долгота).
        private static final float MIN_LATITUDE = 40.4774f;
        private static final float MAX_LATITUDE = 40.9176f;
        private static final float MIN_LONGITUDE = -74.2591f;
        private static final float MAX_LONGITUDE = -73.7004f;

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            // Проверяем, находятся ли начальная и конечная точки поездки в пределах Нью-Йорка.
            return isWithinNYC(taxiRide.startLat, taxiRide.startLon) && isWithinNYC(taxiRide.endLat, taxiRide.endLon);
        }

        // Проверяет, находится ли точка в пределах Нью-Йорка.
        private boolean isWithinNYC(float latitude, float longitude) {
            return latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE && longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
        }
    }
}
