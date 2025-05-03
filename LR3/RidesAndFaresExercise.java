package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Упражнение по обогащению данных о поездках TaxiRides соответствующими данными о тарифах TaxiFares
 */
public class RidesAndFaresExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        // Извлекаем параметры из входных аргументов.
        ParameterTool params = ParameterTool.fromArgs(args);
        // Путь к входному файлу с данными о поездках.  По умолчанию используется ExerciseBase.pathToRideData.
        String ridesInput = params.get("rides", pathToRideData);
        // Путь к входному файлу с данными о тарифах. По умолчанию используется ExerciseBase.pathToFareData.
        String faresInput = params.get("fares", pathToFareData);

        // Параметры конфигурации: максимальная задержка события и коэффициент ускорения.
        int maxDelay = 60;
        int speedFactor = 1800;

        // Настраиваем среду выполнения Flink с контрольными точками и веб-интерфейсом.
        Configuration configuration = new Configuration();
        configuration.setString("state.backend", "filesystem"); // Тип хранилища состояния
        configuration.setString("state.savepoints.dir", "file - FORBIDDEN - /tmp/savepoints"); // Директория для сохранения контрольных точек
        configuration.setString("state.checkpoints.dir", "file - FORBIDDEN - /tmp/checkpoints"); // Директория для контрольных точек
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration); // Создаем локальную среду с веб-интерфейсом
        env.setParallelism(ExerciseBase.parallelism); // Устанавливаем уровень параллелизма

        // Включаем контрольные точки и настраиваем их параметры.
        env.enableCheckpointing(10000L); // Период создания контрольных точек (мс)
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // Сохраняем контрольные точки при отмене

        // Создаем поток данных о поездках.
        DataStream<TaxiRide> rideStream = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesInput, maxDelay, speedFactor)))
                .filter(ride -> ride.isStart) // Фильтруем, оставляя только начало поездок.
                .keyBy(ride -> ride.rideId); // Группируем по ID поездки.

        // Создаем поток данных о тарифах.
        DataStream<TaxiFare> fareStream = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresInput, maxDelay, speedFactor)))
                .keyBy(fare -> fare.rideId); // Группируем по ID поездки.

        // Объединяем потоки и обогащаем данные о поездках данными о тарифах.
        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedStream = rideStream
                .connect(fareStream)
                .flatMap(new RideFareEnrichmentFunction())
                .uid("ride-fare-enrichment"); // Устанавливаем уникальный ID для оператора

        // Выводим или тестируем обогащенный поток.
        printOrTest(enrichedStream);

        // Запускаем выполнение.
        env.execute("Enrich Taxi Rides with Fares");
    }


    // Внутренний класс для обогащения данных о поездках данными о тарифах.
    public static class RideFareEnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Состояния для хранения данных о поездке и тарифе.
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration parameters) {
            // Инициализируем состояния.
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("ride-state", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("fare-state", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            // Обработка TaxiRide.
            TaxiFare fare = fareState.value();

            // Если тариф еще
            не найден, сохраняем данные о поездке в состоянии.
            if (fare == null) {
                rideState.update(ride);
                return;
            }

            // Если тариф найден, формируем пару (поездка, тариф) и очищаем состояние.
            fareState.clear();
            collector.collect(Tuple2.of(ride, fare));
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
            // Обработка TaxiFare.
            TaxiRide ride = rideState.value();

            // Если поездка еще не найдена, сохраняем данные о тарифе в состоянии.
            if (ride == null) {
                fareState.update(fare);
                return;
            }

            // Если поездка найдена, формируем пару (поездка, тариф) и очищаем состояние.
            rideState.clear();
            collector.collect(Tuple2.of(ride, fare));
        }
    }
}

