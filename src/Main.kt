import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*
import kotlin.random.Random

// Модель данных
data class SensorReading(
    val sensorId: String,
    val value: Double,
    val timestamp: Long
)

// Корутины + канал: генерирует 10 показаний
suspend fun emitSensorData(channel: SendChannel<SensorReading>, sensorId: String) {
    repeat(10) {
        val value = when (sensorId) {
            "TEMP" -> Random.nextDouble(18.0, 35.0)
            "HUM"  -> Random.nextDouble(30.0, 90.0)
            else   -> Random.nextDouble(300.0, 800.0)
        }
        channel.send(SensorReading(sensorId, value, System.currentTimeMillis()))
        delay(500)
    }
}

// Flow: ленивый поток
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    repeat(10) {
        val value = when (sensorId) {
            "TEMP" -> Random.nextDouble(18.0, 35.0)
            "HUM"  -> Random.nextDouble(30.0, 90.0)
            else   -> Random.nextDouble(300.0, 800.0)
        }
        emit(SensorReading(sensorId, value, System.currentTimeMillis()))
        delay(300)
    }
}

fun main() = runBlocking {
    println("=".repeat(50))
    println("Умная теплица «Вертикаль-9»")
    println("=".repeat(50))

    // ---------- 1. Каналы + корутины ----------
    println("\n1. Каналы + корутины (ручная обработка)")
    println("-".repeat(50))

    val channel = Channel<SensorReading>()

    // Запускаем 3 датчика параллельно
    val jobs = listOf("TEMP", "HUM", "CO2").map { id ->
        launch {
            emitSensorData(channel, id)
        }
    }

    // Закрываем канал после завершения всех датчиков
    launch {
        jobs.joinAll()
        channel.close()
    }

    // Ручная обработка
    var count = 0
    for (reading in channel) {
        count++
        println("  [$count/30] ${reading.sensorId}: ${"%.2f".format(reading.value)}")
    }
    println("  Всего: $count показаний")

    // ---------- 2. Flow — ленивый поток ----------
    println("\n2. Flow — ленивый поток (декларативно)")
    println("-".repeat(50))

    val tempFlow = sensorFlow("TEMP")
    println("  Поток создан, но ещё не выполняется")

    val result = tempFlow
        .filter { it.value > 20.0 }
        .map { it.copy(value = it.value * 1.1) }
        .take(5)
        .onEach { println("  Обработано: ${it.sensorId} = ${"%.2f".format(it.value)}") }
        .toList()   // терминальная операция — поток запускается здесь

    println("  Собрано ${result.size} показаний")

    // ---------- 3. Агрегация через fold ----------
    println("\n3. Агрегация через fold")
    println("-".repeat(50))

    val (sum, cnt) = sensorFlow("TEMP")
        .filter { it.value > 20.0 }
        .map { it.copy(value = it.value * 1.1) }
        .take(5)
        .fold(0.0 to 0) { (s, c), r ->
            (s + r.value) to (c + 1)
        }

    val avg = if (cnt > 0) sum / cnt else 0.0
    println("  Сумма: ${"%.2f".format(sum)}")
    println("  Количество: $cnt")
    println("  Среднее: ${"%.2f".format(avg)}")

    // ---------- Сравнение ----------
    println("\n" + "=".repeat(50))
    println("Разница:")
    println("  Канал  — горячий, ручная обработка, нужно закрывать")
    println("  Flow   — холодный (ленивый), декларативный, запускается только при collect/toList/fold")
}