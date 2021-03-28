# Реализация консистентного хеширования

В этом задании вы реализуете алгоритмы добавления узла, удаления узла и поиска узла по ключу в консистентном хешировании. 

## Постановка задачи

В файле [`ConsistentHash.kt`](src/main/kotlin/ConsistentHash.kt) находится описание интерфейса, который вам предстоит реализовать.
Свой код вы должны писать на языке Kotlin в файле [`ConsistentHashImpl.kt`](src/main/kotlin/ConsistentHashImpl.kt). 

Допустима также реализация на Java. Для этого, как и в предыдущих заданиях, удалите `ConsistentHashImpl.kt` и вместо него напишите файл
`ConsistentHashImpl.java` с классом `ConsistentHashImpl<K>`, который реализует интерфейс `ConsistentHash<K>`.

### Вспомогательные классы
Вспомогательные классы описаны в файлах [`Shard.kt`](src/main/kotlin/Shard.kt) и [`HashRange.kt`](src/main/kotlin/HashRange.kt).

Класс `Shard` описывает узел системы и задаётся своим именем. Класс `HashRange` описывает отрезок хешей, перемещаемых с существующего узла на новый узел
(или с удаляемого узла на на неудаляемый узел), и задаётся своими левой и правой границами (обе границы включены в диапазон). 


### Описание операций

В этом задании вам предстоит реализовать три операции:

* `fun getShardByKey(key: K): Shard` &mdash; возвращается по ключу узел системы, отвечающий за этот ключ. Гарантируется, что в момент этого вызова в системе
существует хотя бы один узел. `K` - тип ключей, хранящихся в системе. Для получения хеша по ключу необходимо пользоваться методом 
[hashCode](https://docs.oracle.com/javase/8/docs/api/java/lang/Object.html#hashCode--).
* `fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>>` &mdash; добавляет новый узел в систему. Метод возвращает отображение,
в котором  для каждого существующего узла, который должен передать новому узлу **хотя бы один** отрезок хешей, сопоставлено множество отрезков хешей,
которые он должен передать новому узлу. Гарантируется, что узла с таким именем в системе не существует, также
в системе не существует ни одной точки из `vnodeHashes`.
* `fun removeShard(shard: Shard): Map<Shard, Set<HashRange>>` &mdash; удаляет существующий узел.
Метод возвращает отображение,
в котором  для каждого неудаляемого узла узла, которому удаляемый узел должен передать **хотя бы один** отрезок хешей, сопоставлено множество отрезков хешей,
которые ему должен передать удаляемый узел.
Гарантируется, что удаляемый узел существует в системе, и что, 
помимо удаляемого узла, в системе существует ещё хотя бы один неудаляемый узел.

## Тестирование

Тестирования реализации происходит путем запуска тестов [`ConsistentHashImplUnitTest`](src/test/kotlin/ConsistentHashImplUnitTest.kt) и
[`ConsistentHashImplStressTest`](src/test/kotlin/ConsistentHashImplStressTest.kt). 
Из командной строки: `./gradlew test`. 

* unit-тест проверяет несколько базовых сценариев корректности вашего решения
* stress-тест выполняет набор случайных тестов из папки [`tests`](src/test/resources)

## Формат сдачи

Выполняйте задание в этом репозитории. 
**Код процесса должен быть реализован в одном файле [`ConsistentHashImpl.kt`](src/main/kotlin/ConsistentHashImpl.kt) или
`src/main/java/ConsistentHashImpl.java`**.

Инструкции по сдаче заданий находятся в 
[этом документе](https://docs.google.com/document/d/1GQ0OI_OBkj4kyOvhgRXfacbTI9huF4XJDMOct0Lh5og). 


