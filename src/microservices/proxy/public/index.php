<?php
require_once __DIR__ . '/../vendor/autoload.php';
// require_once __DIR__ . '/../config/database.php';

use ProxyService\Controllers\ApiController;
// use DeviceService\Services\DeviceService;
use Slim\Factory\AppFactory;

$app = AppFactory::create();
$app->addBodyParsingMiddleware();
$app->addErrorMiddleware(true, true, true);

// $apiController = new ApiService();
$apiController = new ApiController();

// Правильные маршруты
$app->get('/api/movies', [$apiController, 'getMovies']);
$app->get('/api/users', [$apiController, 'getUsers']);
$app->get('/health', function ($request, $response) {
    $response->getBody()->write(json_encode(['status' => true]));
    return $response->withHeader('Content-Type', 'application/json');
});


$app->run();
