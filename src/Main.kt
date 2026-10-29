import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*
import kotlin.random.Random

// ============================================================
// 1. МОДЕЛЬ ДАННЫХ
// ============================================================
data class SensorReading(
    val sensorId: String,
    val value: Double,
    val timestamp: Long
)

// ============================================================
// 2. КОРУТИНЫ: ПАРАЛЛЕЛЬНАЯ ГЕНЕРАЦИЯ ЧЕРЕЗ КАНАЛ
// ============================================================
suspend fun emitSensorData(channel: SendChannel<SensorReading>) {
    val sensorId = when (Random.nextInt(3)) {
        0 -> "TEMP"
        1 -> "HUM"
        else -> "CO2"
    }
    repeat(10) { i ->
        val value = when (sensorId) {
            "TEMP" -> Random.nextDouble(18.0, 35.0)   // температура
            "HUM" -> Random.nextDouble(30.0, 90.0)    // влажность
            else -> Random.nextDouble(300.0, 800.0)   // CO2
        }
        val reading = SensorReading(
            sensorId = sensorId,
            value = value,
            timestamp = System.currentTimeMillis()
        )
        channel.send(reading)
        delay(500)
    }
}

// ============================================================
// 3. FLOW: ЛЕНИВЫЙ ПОТОК С ОПЕРАЦИЯМИ
// ============================================================
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    repeat(10) { i ->
        val value = when (sensorId) {
            "TEMP" -> Random.nextDouble(18.0, 35.0)
            "HUM" -> Random.nextDouble(30.0, 90.0)
            else -> Random.nextDouble(300.0, 800.0)
        }
        emit(SensorReading(sensorId, value, System.currentTimeMillis()))
        delay(300)
    }
}

// ============================================================
// 4. MAIN
// ============================================================
fun main() = runBlocking {
    println("=".repeat(60))
    println("🌱 УМНАЯ ТЕПЛИЦА «ВЕРТИКАЛЬ-9»")
    println("=".repeat(60))

    // ============================================================
    // ЧАСТЬ 1: КАНАЛЫ + КОРУТИНЫ
    // ============================================================
    println("\n📡 ЧАСТЬ 1: КАНАЛЫ + КОРУТИНЫ (ручная обработка)")
    println("-".repeat(60))

    val channel = Channel<SensorReading>()

    // Запускаем 3 корутины-датчика параллельно
    val sensors = listOf("TEMP", "HUM", "CO2")
    val jobs = sensors.map { sensorId ->
        launch(Dispatchers.Default) {
            println("  🔧 Запущен датчик: $sensorId")
            repeat(10) { i ->
                val value = when (sensorId) {
                    "TEMP" -> Random.nextDouble(18.0, 35.0)
                    "HUM" -> Random.nextDouble(30.0, 90.0)
                    else -> Random.nextDouble(300.0, 800.0)
                }
                channel.send(SensorReading(sensorId, value, System.currentTimeMillis()))
                delay(500)
            }
        }
    }

    // Закрываем канал, когда все датчики закончат
    launch {
        jobs.forEach { it.join() }
        channel.close()
        println("\n  📪 Канал закрыт. Все датчики завершили работу.")
    }

    // Ручная обработка — читаем из канала в цикле
    var totalReadings = 0
    for (reading in channel) {
        totalReadings++
        println("  📊 [${totalReadings}/30] ${reading.sensorId}: " +
                "${"%.2f".format(reading.value)} (t=${reading.timestamp})")
    }
    println("  ✅ Всего обработано: $totalReadings показаний")

    // ============================================================
    // ЧАСТЬ 2: FLOW — ЛЕНИВЫЙ ПОТОК
    // ============================================================
    println("\n🌊 ЧАСТЬ 2: FLOW — ЛЕНИВЫЙ ПОТОК (декларативно)")
    println("-".repeat(60))

    // Создаём поток, но НЕ запускаем
    val tempFlow = sensorFlow("TEMP")
    println("  🔹 Поток создан, но ещё НЕ выполняется")
    println("  🔹 Сейчас вызовем toList() — поток начнёт работу")

    // Цепочка операций
    val filteredFlow = tempFlow
        .filter { it.value > 20.0 }          // отбросить низкие
        .map { it.copy(value = it.value * 1.1) } // калибровка
        .take(5)                             // первые 5 подходящих
        .onEach { println("  🔹 Обработано: ${it.sensorId} = " +
                "${"%.2f".format(it.value)}") }

    // Терминальная операция — запускает поток
    println("\n  🔹 Запуск toList()...")
    val collected = filteredFlow.toList()
    println("  ✅ Собрано ${collected.size} показаний:")
    collected.forEach { println("     ${it.sensorId}: ${"%.2f".format(it.value)}") }

    // ============================================================
    // ЧАСТЬ 3: АГРЕГАЦИЯ ЧЕРЕЗ FOLD
    // ============================================================
    println("\n📈 ЧАСТЬ 3: АГРЕГАЦИЯ ЧЕРЕЗ FOLD")
    println("-".repeat(60))

    val newFlow = sensorFlow("TEMP")
        .filter { it.value > 20.0 }
        .map { it.copy(value = it.value * 1.1) }
        .take(5)

    println("  🔹 Поток создан, но не выполнен")
    println("  🔹 Запуск fold()...")

    val (sum, count) = newFlow.fold(0.0 to 0) { acc, reading ->
        val (s, c) = acc
        println("  🔹 Накоплено: ${"%.2f".format(s + reading.value)} (${c + 1} шт.)")
        (s + reading.value) to (c + 1)
    }

    val average = if (count > 0) sum / count else 0.0
    println("\n  ✅ Сумма: ${"%.2f".format(sum)}")
    println("  ✅ Количество: $count")
    println("  ✅ Среднее: ${"%.2f".format(average)}")

    // ============================================================
    // СРАВНЕНИЕ
    // ============================================================
    println("\n" + "=".repeat(60))
    println("📝 СРАВНЕНИЕ ПОДХОДОВ")
    println("=".repeat(60))
    println("""
    ┌─────────────────────┬──────────────────────────────────────┐
    │ КАНАЛЫ (корутины)   │ FLOW                                 │
    ├─────────────────────┼──────────────────────────────────────┤
    │ Ручная обработка    │ Декларативная цепочка операций       │
    │ for (msg in channel)│ .filter().map().take().onEach()      │
    │                     │                                      │
    │ Горячий поток       │ Холодный поток                       │
    │ (работает сразу)    │ (только при терминальной операции)   │
    │                     │                                      │
    │ Нужно управлять     │ Ленивый — не работает, пока          │
    │ закрытием канала    │ не вызовешь collect/toList/fold      │
    │                     │                                      │
    │ Один потребитель    │ Много операторов                     │
    │                     │ (легко комбинировать)                │
    │                     │                                      │
    │ Подходит для:       │ Подходит для:                        │
    │ - обмена между      │ - обработки потоков данных           │
    │   корутинами        │ - декларативных преобразований       │
    │ - сложной логики    │ - ленивых вычислений                 │
    └─────────────────────┴──────────────────────────────────────┘
    
    📌 Принципиальная разница:
    
    КАНАЛ — это «труба» для передачи данных между корутинами.
    Ты сам решаешь, когда читать, как обрабатывать, когда закрыть.
    Это императивный подход — «сделай шаг 1, шаг 2, шаг 3».
    
    FLOW — это «река данных» с фильтрами.
    Ты описываешь, ЧТО делать с данными (filter, map, take),
    а не КАК. Поток ленивый — начинает работу только при
    терминальной операции (collect, toList, fold).
    Это декларативный подход — «опиши результат».
    """.trimIndent())

    println("=".repeat(60))
    println("🌱 Мониторинг теплицы завершён")
    println("=".repeat(60))
}