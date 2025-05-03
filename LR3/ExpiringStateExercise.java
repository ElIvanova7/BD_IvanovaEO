package com.ververica.flinktraining.exercises.datastream_java.process;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
/**
 * The "Expiring State" упражнение из the Flink training
 *
 * Цель этого упражнения — обогатить данные о поездках TaxiRides информацией о тарифах.
 *
 * Параметры:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
    // Тег для вывода несопоставленных поездок
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
    // Тег для вывода несопоставленных тарифов
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

    public static void main(String[] args) throws Exception {

        // Получение параметров из командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        // Путь к файлу с данными о поездках
        final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
        // Путь к файлу с данными о тарифах
        final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

        // Максимальная задержка события (в секундах)
        final int maxEventDelay = 60;
        // Коэффициент скорости обработки событий (для симуляции реального времени)
        final int servingSpeedFactor = 600;

        // Настройка среды выполнения потоковой обработки
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // Создание потока данных о поездках
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0))) //Фильтруем поездки
                .keyBy(ride -> ride.rideId); //Группируем по ID поездки

        // Создание потока данных о тарифах
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId); //Группируем по ID поездки

        // Объединение потоков и обработка данных
        SingleOutputStreamOperator processed = rides
                .connect(fares)
                .process(new EnrichmentFunction());

        // Вывод несопоставленных тарифов
        printOrTest(processed.getSideOutput(unmatchedFares));

        env.execute("ExpiringStateExercise (java)");
    }

    public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Состояние для хранения данных о поездке
        private ValueState<TaxiRide> rideState;
        // Состояние для хранения данных о тарифе
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Инициализация состояний
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Получение данных о тарифе из состояния
            TaxiFare fare = fareState.value();

            // Если данные о тарифе отсутствуют, сохраняем данные о поездке и устанавливаем таймер
            if (fare == null) {
                rideState.update(ride);
                context.timerService().registerEventTimeTimer(ride.getEventTime());
                return;
            }

            // Если данные о тарифе есть, очищаем состояние и отправляем результат
            fareState.clear();
            context.timerService().deleteEventTimeTimer(fare.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        }

        @Override
        public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Получение данных о поездке из состояния
            TaxiRide ride =
                    rideState.value();

            // Если данные о поездке отсутствуют, сохраняем данные о тарифе и устанавливаем таймер
            if (ride == null) {
                fareState.update(fare);
                context.timerService().registerEventTimeTimer(fare.getEventTime());
                return;
            }
            // Если данные о поездке есть, очищаем состояние и отправляем результат
            rideState.clear();
            context.timerService().deleteEventTimeTimer(ride.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка таймера: отправка несопоставленных данных

            if (fareState.value() != null) {
                TaxiFare fare = fareState.value();
                ctx.output(unmatchedFares, fare);
                fareState.clear();
            }

            if (rideState.value() != null) {
                TaxiRide ride = rideState.value();
                ctx.output(unmatchedRides, ride);
                rideState.clear();
            }
        }
    }
}

