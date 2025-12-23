<?php

namespace ProxyService\Controllers;

use Psr\Http\Message\ResponseInterface as Response;
use Psr\Http\Message\ServerRequestInterface as Request;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use GuzzleHttp\Client;

class ApiController
{
    public int $moviesMigrationPercent;
    public bool $gradualMigration;
    public string $monolithUrl;
    public string $moviesServiceUrl;
    private Client $httpClient;

    public function __construct()
    {
        $this->moviesMigrationPercent = (int)$this->getEnv('MOVIES_MIGRATION_PERCENT', '0');
        $this->gradualMigration = $this->getEnv('GRADUAL_MIGRATION', 'false') === 'true';
        $this->monolithUrl = $this->getEnv('MONOLITH_URL', 'http://monolith:8080');
        $this->moviesServiceUrl = $this->getEnv('MOVIES_SERVICE_URL', 'http://movies-service:8081');

        // Создаем HTTP клиент с общими настройками
        $this->httpClient = new Client([
            'timeout' => 30.0,
            'connect_timeout' => 5.0,
            'allow_redirects' => false, // Обрабатываем редиректы вручную
            'http_errors' => false, // Не выбрасываем исключения при 4xx/5xx
            'verify' => false, // Отключаем проверку SSL для демо (в продакшене лучше включить)
        ]);
    }

    public function getMovies(Request $request, Response $response): Response
    {
        $targetResponse = $this->forwardRequest($this->shouldMigrate() ? $this->moviesServiceUrl : $this->monolithUrl);

        return $this->createResponseFromTarget($response, $targetResponse);
    }

    public function getUsers(Request $request, Response $response): Response
    {
        $targetResponse = $this->forwardRequest($this->monolithUrl);

        return $this->createResponseFromTarget($response, $targetResponse);
    }

    private function getEnv(string $key, string $default): string
    {
        $value = getenv($key);
        return $value !== false ? $value : $default;
    }

    private function shouldMigrate(): bool
    {
        if (!$this->gradualMigration) {
            return false;
        }

        // Генерируем число от 0 до 99
        $roll = random_int(0, 99);
        return $roll < $this->moviesMigrationPercent;
    }

    private function forwardRequest(string $baseUrl = '')
    {

        $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        $path = $_SERVER['REQUEST_URI'] ?? '/';

        $url = $baseUrl . $path;

        // Добавляем query параметры, если есть
        if (!empty($_SERVER['QUERY_STRING'])) {
            $url .= '?' . $_SERVER['QUERY_STRING'];
        }

        $headers = [];        

        // Читаем тело запроса
        $requestBody = file_get_contents('php://input');

        // Создаем PSR-7 запрос
        $request = new GuzzleRequest($method, $url, $headers, $requestBody);

        // Отправляем запрос и возвращаем ответ
        return $this->httpClient->send($request);
    }

    private function createResponseFromTarget(Response $baseResponse, Response $targetResponse): Response
    {
        // Устанавливаем статус код
        $baseResponse = $baseResponse->withStatus($targetResponse->getStatusCode());

        // Копируем заголовки из целевого ответа
        foreach ($targetResponse->getHeaders() as $name => $values) {
            // Пропускаем некоторые заголовки, которые должны быть установлены PHP
            $skipHeaders = ['transfer-encoding', 'connection', 'keep-alive'];
            if (in_array(strtolower($name), $skipHeaders)) {
                continue;
            }

            $baseResponse = $baseResponse->withHeader($name, $values);
        }

        // Копируем тело ответа
        $baseResponse->getBody()->write($targetResponse->getBody()->getContents());

        return $baseResponse;
    }
}
