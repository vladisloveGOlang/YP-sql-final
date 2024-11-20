package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

//добавляет новую строку в таблцу по аргументу, возвращает number
func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	db, err := sql.Open("sqlite", "tracker.db")
	if err != nil {
		return 0, err
	}
	defer db.Close()
	query := "INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?);"
	sqlRes, err := db.Exec(query, p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, err
	}

	id, err := sqlRes.LastInsertId()
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	return int(id), nil
}

//Выводит 1 строку в струтуре Parcel  
func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	db, err := sql.Open("sqlite", "tracker.db")
	if err != nil {
		return Parcel{}, err
	}
	defer db.Close()
	// заполните объект Parcel данными из таблицы
	query := "SELECT * FROM parcel WHERE number = ?;"
	row := db.QueryRow(query, number)

	var p Parcel

	err = row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	db, err := sql.Open("sqlite", "tracker.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	// заполните срез Parcel данными из таблицы
	query := "SELECT * FROM parcel WHERE client = ?;"
	rows, err := db.Query(query, client)
	if err != nil {
		return nil, err
	}
	var res []Parcel
	for rows.Next() {
		var p Parcel
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		defer rows.Close()
		if err != nil {
			err = fmt.Errorf("ошибка при записи слайса данных %w", err)
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	db, err := sql.Open("sqlite", "tracker.db")
	defer db.Close()
	if err != nil {
		err = fmt.Errorf("ошибка при подключении к базе %w", err)
		return err
	}
	query := "UPDATE parcel SET status= ? WHERE number = ?;"
	_, err = db.Exec(query, status, number)
	if err != nil {
		err = fmt.Errorf("ошибка при update запросе %w", err)
		return err
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	// реализуйте обновление статуса в таблице parcel
	db, err := sql.Open("sqlite", "tracker.db")
	defer db.Close()
	if err != nil {
		err = fmt.Errorf("ошибка при подключении к базе %w", err)
		return err
	}
	p, err := s.Get(number)
	if err != nil {
		err = fmt.Errorf("ошибка при получении строки в SetAddress %w", err)
		return err
	}
	if p.Status != "registered" {
		err = fmt.Errorf("попытка обноваления записи со статусом != registered ")
		return err
	}

	query := "UPDATE parcel SET address = ? WHERE number = ?;"
	_, err = db.Exec(query, address, number)
	if err != nil {
		err = fmt.Errorf("ошибка при обновлении адреса в SetAddress %w", err)
		return err
	}

	pNew, err := s.Get(number)
	if err != nil {
		err = fmt.Errorf("ошибка при повторном получении строки в SetAddress %w", err)
		return err
	}

	if pNew.Address != address {
		err = fmt.Errorf("Новый адрес не совпадает с аргументом в SetAddress")
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	db, err := sql.Open("sqlite", "tracker.db")
	defer db.Close()
	if err != nil {
		err = fmt.Errorf("ошибка при подключении к базе %w", err)
		return err
	}
	p, err := s.Get(number)
	if err != nil {
		err = fmt.Errorf("ошибка при получении строки в Delete %w", err)
		return err
	}
	if p.Status != "registered" {
		fmt.Println("попытка удаления отправленой посылки отменена")
		return nil
	}
	query := "DELETE FROM parcel WHERE number=?;"
	_, err = db.Exec(query, number)
	if err != nil {
		err = fmt.Errorf("В Delete ошибка при удалении строки %w", err)
		return err
	}
	_, err = s.Get(number)
	if err != sql.ErrNoRows {
		err = fmt.Errorf("в Delete нужная строка не удалилась %w", err)
		return err
	}

	return nil
}
