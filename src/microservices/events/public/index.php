<?php

use Psr\Http\Message\ResponseInterface as Response;
use Psr\Http\Message\ServerRequestInterface as Request;
use Slim\Factory\AppFactory;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use RdKafka\Producer;
use RdKafka\Consumer;
use RdKafka\Conf;
use RdKafka\TopicConf;

require_once __DIR__ . '/../vendor/autoload.php';

// Инициализация приложения
$app = AppFactory::create();
$app->addBodyParsingMiddleware();
$app->addErrorMiddleware(true, true, true);

// Логирование
$log = new Logger('events-service');
$log->pushHandler(new StreamHandler(__DIR__ . '/logs/app.log', Logger::DEBUG));

// Класс для работы с Kafka
class KafkaService
{
    private Producer $producer;
    private Consumer $consumer;
    private string $broker;
    private Logger $logger;

    public function __construct(Logger $logger)
    {
        $this->logger = $logger;
        $this->broker = getenv('KAFKA_BROKERS') ?: 'kafka:9092';
        $this->initProducer();
        $this->initConsumer();
        $this->startConsumers();
    }

    private function initProducer(): void
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->broker);

        $this->producer = new Producer($conf);
        $this->logger->info('Kafka producer initialized');
    }

    private function initConsumer(): void
    {
        $conf = new Conf();
        $conf->set('group.id', 'events-service-group');
        $conf->set('metadata.broker.list', $this->broker);
        $conf->set('auto.offset.reset', 'latest');

        $this->consumer = new Consumer($conf);
        $this->logger->info('Kafka consumer initialized');
    }

    public function produce(string $topic, string $message): array
    {
        $kafkaTopic = $this->producer->newTopic($topic);
        $kafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
        $this->producer->poll(0);

        $result = $this->producer->flush(10000);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new RuntimeException('Failed to flush Kafka messages');
        }

        $this->logger->info("Event sent to topic: $topic");

        return ['partition' => 0, 'offset' => 0];
    }

    private function startConsumers(): void
    {
        $topics = ['movie-events', 'user-events', 'payment-events'];

        // В реальном приложении лучше запускать в отдельных процессах
        foreach ($topics as $topic) {
            $this->consumeTopic($topic);
        }
    }

    private function consumeTopic(string $topicName): void
    {
        // Запускаем в фоновом потоке (для демо - в основном потоке)
        $this->logger->info("Starting consumer for topic: $topicName");

        // В реальном приложении используйте Supervisor или отдельный процесс
        if (extension_loaded('pcntl')) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                die('Could not fork');
            } elseif ($pid) {
                // Родительский процесс
                return;
            }

            // Дочерний процесс для консьюмера
            $this->runConsumer($topicName);
            exit(0);
        }
    }

    private function runConsumer(string $topicName): void
    {
        $topicConf = new TopicConf();
        $topicConf->set('auto.offset.reset', 'latest');

        $topic = $this->consumer->newTopic($topicName, $topicConf);
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

        $this->logger->info("Started consuming from topic: $topicName");

        while (true) {
            $message = $topic->consume(0, 1000);

            if (null === $message) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processMessage($topicName, $message->payload);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->logger->debug("No more messages in topic $topicName");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->logger->debug("Timeout consuming from topic $topicName");
                    break;
                default:
                    $this->logger->error("Error consuming from topic $topicName: " . $message->errstr());
                    break;
            }
        }
    }

    private function processMessage(string $topic, string $message): void
    {
        $data = json_decode($message, true);

        if (!$data) {
            $this->logger->error("Invalid JSON from topic $topic");
            return;
        }

        switch ($topic) {
            case 'movie-events':
                $this->logger->info("Processing movie event", $data);
                // Бизнес-логика для фильмов
                break;
            case 'user-events':
                $this->logger->info("Processing user event", $data);
                // Бизнес-логика для пользователей
                break;
            case 'payment-events':
                $this->logger->info("Processing payment event", $data);
                // Бизнес-логика для платежей
                break;
        }

        // Сохраняем в базу (опционально)
        $this->saveToDatabase($data, $topic);
    }

    private function saveToDatabase(array $event, string $topic): void
    {
        // Реализация сохранения в базу
        // Например, через PDO
        try {
            $pdo = new PDO(
                getenv('DB_DSN') ?: 'mysql:host=localhost;dbname=events_db',
                getenv('DB_USER') ?: 'root',
                getenv('DB_PASS') ?: ''
            );

            $stmt = $pdo->prepare(
                "INSERT INTO events (event_id, type, payload, timestamp, topic, created_at) 
                 VALUES (:event_id, :type, :payload, :timestamp, :topic, NOW())"
            );

            $stmt->execute([
                ':event_id' => $event['id'],
                ':type' => $event['type'],
                ':payload' => json_encode($event['payload']),
                ':timestamp' => date('Y-m-d H:i:s', strtotime($event['timestamp'])),
                ':topic' => $topic,
            ]);
        } catch (PDOException $e) {
            $this->logger->error("Database error: " . $e->getMessage());
        }
    }
}

// Инициализация Kafka
$kafkaService = new KafkaService($log);

// Валидация
function validateMovieEvent(array $data): array
{
    $errors = [];

    if (empty($data['movie_id'])) $errors[] = 'movie_id is required';
    if (empty($data['title'])) $errors[] = 'title is required';
    if (empty($data['action'])) $errors[] = 'action is required';

    $allowedActions = ['viewed', 'rated', 'added'];
    if (!empty($data['action']) && !in_array($data['action'], $allowedActions)) {
        $errors[] = 'action must be one of: ' . implode(', ', $allowedActions);
    }

    return $errors;
}

function validateUserEvent(array $data): array
{
    $errors = [];

    if (empty($data['user_id'])) $errors[] = 'user_id is required';
    if (empty($data['action'])) $errors[] = 'action is required';

    $allowedActions = ['registered', 'logged_in', 'updated_profile'];
    if (!empty($data['action']) && !in_array($data['action'], $allowedActions)) {
        $errors[] = 'action must be one of: ' . implode(', ', $allowedActions);
    }

    if (!empty($data['email']) && !filter_var($data['email'], FILTER_VALIDATE_EMAIL)) {
        $errors[] = 'email is invalid';
    }

    return $errors;
}

function validatePaymentEvent(array $data): array
{
    $errors = [];

    if (empty($data['payment_id'])) $errors[] = 'payment_id is required';
    if (empty($data['user_id'])) $errors[] = 'user_id is required';
    if (empty($data['amount'])) $errors[] = 'amount is required';
    if (empty($data['status'])) $errors[] = 'status is required';

    $allowedStatuses = ['completed', 'failed', 'refunded'];
    if (!empty($data['status']) && !in_array($data['status'], $allowedStatuses)) {
        $errors[] = 'status must be one of: ' . implode(', ', $allowedStatuses);
    }

    if (!empty($data['amount']) && $data['amount'] < 0) {
        $errors[] = 'amount must be positive';
    }

    return $errors;
}

// Маршруты
$app->get('/api/events/health', function (Request $request, Response $response) {
    $response->getBody()->write(json_encode(['status' => true]));
    return $response->withHeader('Content-Type', 'application/json');
});

$app->post('/api/events/movie', function (Request $request, Response $response) use ($kafkaService, $log) {
    $data = $request->getParsedBody();
    $errors = validateMovieEvent($data);

    if (!empty($errors)) {
        $response->getBody()->write(json_encode([
            'error' => 'Validation failed',
            'details' => $errors
        ]));
        return $response
            ->withStatus(400)
            ->withHeader('Content-Type', 'application/json');
    }

    $movieEvent = [
        'movie_id' => (int)$data['movie_id'],
        'title' => $data['title'],
        'action' => $data['action'],
        'user_id' => isset($data['user_id']) ? (int)$data['user_id'] : null,
        'rating' => isset($data['rating']) ? (float)$data['rating'] : null,
        'genres' => $data['genres'] ?? [],
        'description' => $data['description'] ?? null,
    ];

    $event = [
        'id' => 'movie-' . $movieEvent['movie_id'] . '-' . $movieEvent['action'],
        'type' => 'movie',
        'timestamp' => date('c'),
        'payload' => $movieEvent,
    ];

    try {
        $result = $kafkaService->produce('movie-events', json_encode($event));

        $log->info("Movie event sent", ['event_id' => $event['id']]);

        $response->getBody()->write(json_encode([
            'status' => 'success',
            'partition' => $result['partition'],
            'offset' => $result['offset'],
            'event' => $event,
        ]));

        return $response
            ->withStatus(201)
            ->withHeader('Content-Type', 'application/json');
    } catch (Exception $e) {
        $log->error("Failed to send movie event", ['error' => $e->getMessage()]);

        $response->getBody()->write(json_encode([
            'error' => 'Internal server error',
            'message' => $e->getMessage()
        ]));

        return $response
            ->withStatus(500)
            ->withHeader('Content-Type', 'application/json');
    }
});

$app->post('/api/events/user', function (Request $request, Response $response) use ($kafkaService, $log) {
    $data = $request->getParsedBody();
    $errors = validateUserEvent($data);

    if (!empty($errors)) {
        $response->getBody()->write(json_encode([
            'error' => 'Validation failed',
            'details' => $errors
        ]));
        return $response
            ->withStatus(400)
            ->withHeader('Content-Type', 'application/json');
    }

    $userEvent = [
        'user_id' => (int)$data['user_id'],
        'username' => $data['username'] ?? null,
        'email' => $data['email'] ?? null,
        'action' => $data['action'],
        'timestamp' => date('c'),
    ];

    $event = [
        'id' => 'user-' . $userEvent['user_id'] . '-' . $userEvent['action'],
        'type' => 'user',
        'timestamp' => date('c'),
        'payload' => $userEvent,
    ];

    try {
        $result = $kafkaService->produce('user-events', json_encode($event));

        $log->info("User event sent", ['event_id' => $event['id']]);

        $response->getBody()->write(json_encode([
            'status' => 'success',
            'partition' => $result['partition'],
            'offset' => $result['offset'],
            'event' => $event,
        ]));

        return $response
            ->withStatus(201)
            ->withHeader('Content-Type', 'application/json');
    } catch (Exception $e) {
        $log->error("Failed to send user event", ['error' => $e->getMessage()]);

        $response->getBody()->write(json_encode([
            'error' => 'Internal server error',
            'message' => $e->getMessage()
        ]));

        return $response
            ->withStatus(500)
            ->withHeader('Content-Type', 'application/json');
    }
});

$app->post('/api/events/payment', function (Request $request, Response $response) use ($kafkaService, $log) {
    $data = $request->getParsedBody();
    $errors = validatePaymentEvent($data);

    if (!empty($errors)) {
        $response->getBody()->write(json_encode([
            'error' => 'Validation failed',
            'details' => $errors
        ]));
        return $response
            ->withStatus(400)
            ->withHeader('Content-Type', 'application/json');
    }

    $paymentEvent = [
        'payment_id' => (int)$data['payment_id'],
        'user_id' => (int)$data['user_id'],
        'amount' => (float)$data['amount'],
        'status' => $data['status'],
        'timestamp' => date('c'),
        'method_type' => $data['method_type'] ?? null,
    ];

    $event = [
        'id' => 'payment-' . $paymentEvent['payment_id'] . '-' . $paymentEvent['status'],
        'type' => 'payment',
        'timestamp' => date('c'),
        'payload' => $paymentEvent,
    ];

    try {
        $result = $kafkaService->produce('payment-events', json_encode($event));

        $log->info("Payment event sent", ['event_id' => $event['id']]);

        $response->getBody()->write(json_encode([
            'status' => 'success',
            'partition' => $result['partition'],
            'offset' => $result['offset'],
            'event' => $event,
        ]));

        return $response
            ->withStatus(201)
            ->withHeader('Content-Type', 'application/json');
    } catch (Exception $e) {
        $log->error("Failed to send payment event", ['error' => $e->getMessage()]);

        $response->getBody()->write(json_encode([
            'error' => 'Internal server error',
            'message' => $e->getMessage()
        ]));

        return $response
            ->withStatus(500)
            ->withHeader('Content-Type', 'application/json');
    }
});

// Запуск приложения
$port = getenv('PORT') ?: '8082';
$log->info("Starting events service on port $port");

$app->run();
